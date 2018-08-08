import logging
import sys

import configargparse
import elasticsearch
from elasticsearch import helpers

from db_helpers import get_source_data, check_conn
from es_helpers import make_new_index_name, make_index_config, max_post_id_agg, doc_from_row
from state import get_state, set_state, delete_state

logging.basicConfig(level=logging.INFO)

conf = {}


def work():
    if not check_conn(conf['db_url']):
        logging.error("Could not connect hive db")
        sys.exit(1)

    es = elasticsearch.Elasticsearch(conf['es_url'])

    index_name = make_new_index_name(conf['es_index'])
    min_id = 0

    # State checks
    state = get_state()
    state_flag = False

    if state:
        # Check if index exists
        try:
            es.indices.get(state['index'])
            state_flag = True
        except elasticsearch.exceptions.NotFoundError:
            pass

        # Check last_post_id is still same
        if state_flag:
            res = es.search(index=state['index'], body=max_post_id_agg)
            max_from_index = res['aggregations']['max_post_id']['value'] or 0

            if state['last_post_id'] != int(max_from_index):
                state_flag = False

        if state_flag:
            index_name = state['index']
            min_id = state['last_post_id']
        else:
            delete_state()

    if state_flag:
        logging.info("Resuming from state")
    else:
        logging.info('Creating new index')
        index_config = make_index_config(conf['es_type'])
        es.indices.create(index=index_name, body=index_config)

    logging.info('Starting import')

    set_state(index_name, min_id)

    while True:
        logging.info('Min id: {}'.format(min_id))
        posts = get_source_data(conf['db_url'], conf['bulk_size'], min_id)

        if len(posts) == 0:
            logging.info('Indexing completed')
            break

        index_data = []

        for post in posts:
            min_id = post.post_id

            index_data.append(doc_from_row(post, index_name, conf['es_type']))

        try:
            helpers.bulk(es, index_data)
        except Exception as ex:
            logging.error("Could not complete bulk index. {}".format(str(ex)))
            sys.exit()

        set_state(index_name, min_id)

    es.indices.put_alias(index=index_name, name=conf['es_index'])

    delete_state()

    logging.info('Deleting old index(es)')

    index_pattern = '{}*'.format(conf['es_index'])
    for index in es.indices.get(index_pattern):
        if index != index_name:
            es.indices.delete(index)

    logging.info('Done')


def run():
    parser = configargparse.get_arg_parser()

    parser.add('--db-url', env_var='DB_URL', required=True, help='hive database connection url')

    parser.add('--es-url', env_var='ES_URL', required=True, help='elasticsearch connection url')
    parser.add('--es-index', env_var='ES_INDEX', help='elasticsearch index name', default='hive_index')
    parser.add('--es-type', env_var='ES_TYPE', help='elasticsearch type name', default='posts')

    parser.add('--bulk-size', env_var='BULK_SIZE', type=int, help='Number of records exported in a single loop',
               default=500)

    args = parser.parse_args()

    global conf
    conf = vars(args)

    work()


if __name__ == "__main__":
    run()
