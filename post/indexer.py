import logging
import multiprocessing as mp
import sys
import time

import configargparse
import elasticsearch
from elasticsearch import helpers
from .es_helpers import make_new_index_name, make_index_config, max_post_id_agg, doc_from_row

from post.db_helpers import get_source_data, check_conn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('hive2elastic')

# disable elastic search's confusing logging
logging.getLogger('elasticsearch').setLevel(logging.CRITICAL)

conf = {}
index_name = None
es = None
bulk_errors = 0


def convert_post(row):
    return doc_from_row(row, index_name, conf['es_type'])


def run():
    global conf, es, index_name, bulk_errors

    if not check_conn(conf['db_url']):
        logger.error("Could not connect hive db")
        sys.exit(1)

    es = elasticsearch.Elasticsearch(conf['es_url'])

    # Look for indexes in progress
    indices = es.indices.get_alias('indexing*')

    if len(indices) > 1:
        raise Exception("There are more than 1 indexes in progress.")

    if len(indices) == 1:
        index_name = next(iter(indices))

        res = es.search(index=index_name, body=max_post_id_agg)
        max_from_index = res['aggregations']['max_post_id']['value'] or 0

        min_id = int(max_from_index)

        logger.info('Resuming on index {} from {}'.format(index_name, min_id))
    else:
        index_name = make_new_index_name(conf['es_index'])

        logger.info('Creating new index {}'.format(index_name))

        index_config = make_index_config(conf['es_type'])
        es.indices.create(index=index_name, body=index_config)

        es.indices.put_alias(index=index_name, name='indexing')

        min_id = 0

    logger.info('Starting indexing')

    while True:
        logger.info('Min id: {}'.format(min_id))

        start = time.time()

        posts = get_source_data(conf['db_url'], conf['bulk_size'], min_id)

        if len(posts) == 0:
            logger.info('Indexing completed')
            break

        pool = mp.Pool(processes=conf['max_workers'])
        index_data = pool.map_async(convert_post, posts).get()
        pool.close()
        pool.join()

        try:
            helpers.bulk(es, index_data)
            bulk_errors = 0
        except helpers.BulkIndexError as ex:
            bulk_errors += 1
            logger.error("BulkIndexError occurred. {}".format(ex))

            if bulk_errors >= conf['max_bulk_errors']:
                sys.exit(1)

            time.sleep(1)

            continue

        min_id = posts[-1].post_id

        end = time.time()

        logger.info('{} indexed in {}'.format(len(posts), (end - start)))

    es.indices.put_alias(index=index_name, name=conf['es_index'])
    es.indices.delete_alias(index=index_name, name='indexing')

    logger.info('Deleting old index(es)')

    index_pattern = '{}*'.format(conf['es_index'])
    for index in es.indices.get(index_pattern):
        if index != index_name:
            es.indices.delete(index)

    logger.info('Done')


def main():
    parser = configargparse.get_arg_parser()

    parser.add('--db-url', env_var='DB_URL', required=True, help='hive database connection url')

    parser.add('--es-url', env_var='ES_URL', required=True, help='elasticsearch connection url')
    parser.add('--es-index', env_var='ES_INDEX', help='elasticsearch index name', default='hive_index')
    parser.add('--es-type', env_var='ES_TYPE', help='elasticsearch type name', default='posts')

    parser.add('--bulk-size', env_var='BULK_SIZE', type=int, help='number of records exported in a single loop',
               default=500)

    parser.add('--max-workers', type=int, env_var='MAX_WORKERS', help='max workers', default=2)

    parser.add('--max-bulk-errors', type=int, env_var='MAX_BULK_ERRORS', help='', default=5)

    args = parser.parse_args()

    global conf

    conf = vars(args)

    run()


if __name__ == "__main__":
    main()
