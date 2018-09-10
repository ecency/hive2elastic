"""
Continuously posts indexer. Indexes new records from hive to active index.
"""

import logging
import sys
import time

import configargparse
import elasticsearch
from elasticsearch import helpers

from post.db_helpers import get_source_data, check_conn
from .es_helpers import max_post_id_agg, doc_from_row

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('hive2elastic')

# disable elastic search's confusing logging
logging.getLogger('elasticsearch').setLevel(logging.CRITICAL)

conf = {}
index_name = None
es = None


def convert_post(row):
    return doc_from_row(row, index_name, conf['es_type'])


def run():
    global conf, es, index_name

    if not check_conn(conf['db_url']):
        logger.error("Could not connect hive db")
        sys.exit(1)

    es = elasticsearch.Elasticsearch(conf['es_url'])

    while True:
        try:
            indices = es.indices.get(conf['es_index'])
        except elasticsearch.NotFoundError:
            logger.error("Index not found: {}".format(index_name))
            time.sleep(5)
            continue

        # get real index name from alias
        index_name = next(iter(indices))

        logger.info('Index: {}'.format(index_name))

        try:
            res = es.search(index=index_name, body=max_post_id_agg)
        except elasticsearch.NotFoundError:
            # index removed.
            time.sleep(5)
            continue

        max_from_index = res['aggregations']['max_post_id']['value'] or 0
        min_id = int(max_from_index)

        logger.info('Min id: {}'.format(min_id))

        start = time.time()

        posts = get_source_data(conf['db_url'], conf['bulk_size'], min_id)

        # Nothing new. Wait some.
        if len(posts) == 0:
            time.sleep(5)
            continue

        index_data = [convert_post(x) for x in posts]

        try:
            helpers.bulk(es, index_data)
        except elasticsearch.ElasticsearchException as ex:
            logger.error("Error occurred. {}".format(ex))
            time.sleep(5)
            continue

        end = time.time()

        logger.info('{} indexed in {}'.format(len(posts), (end - start)))

        time.sleep(5)


def main():
    parser = configargparse.get_arg_parser()

    parser.add('--db-url', env_var='DB_URL', required=True, help='hive database connection url')
    parser.add('--es-url', env_var='ES_URL', required=True, help='elasticsearch connection url')
    parser.add('--es-index', env_var='ES_INDEX', help='elasticsearch index name', default='hive_index')
    parser.add('--es-type', env_var='ES_TYPE', help='elasticsearch type name', default='posts')

    args = parser.parse_args()

    global conf

    conf = vars(args)

    run()


if __name__ == "__main__":
    main()
