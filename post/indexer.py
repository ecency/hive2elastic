import logging
import multiprocessing as mp
import sys
import time

import configargparse
import elasticsearch
from elasticsearch import helpers
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError

from .es_helpers import make_index_config, doc_from_row
from .util import chunks

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

    try:
        db_engine = create_engine(conf['db_url'])
        db_engine.execute("SELECT post_id FROM __h2e_posts LIMIT 1")
    except OperationalError:
        raise Exception("Could not connected: {}".format(conf['db_url']))
    except ProgrammingError:
        raise Exception("__h2e_posts table not exists in database")

    es = elasticsearch.Elasticsearch(conf['es_url'])

    if not es.ping():
        raise Exception("Elasticsearch server not reachable")

    index_name = conf['es_index']
    index_type = conf['es_type']

    try:
        es.indices.get(index_name)
    except elasticsearch.NotFoundError:
        logger.info('Creating new index {}'.format(index_name))
        index_config = make_index_config(index_type)
        es.indices.create(index=index_name, body=index_config)

    logger.info('Starting indexing')

    while True:
        sql = '''SELECT t2.post_id, t2.author, t2.permlink, t2.category, t2.depth, t2.children, t2.author_rep,
                 t2.flag_weight, t2.total_votes, t2.up_votes, t2.title, t2.img_url, t2.payout, t2.promoted,
                 t2.created_at, t2.payout_at, t2.updated_at, t2.is_paidout, t2.is_nsfw, t2.is_declined,
                 t2.is_full_power, t2.is_hidden, t2.is_grayed, t2.rshares, t2.sc_hot, t2.sc_trend, t2.sc_hot,
                 t2.body, t2.votes,  t2.json
                 FROM __h2e_posts AS t1 LEFT JOIN hive_posts_cache AS t2 ON t1.post_id = t2.post_id
                 ORDER BY t1.post_id ASC LIMIT :limit '''

        posts = db_engine.execute(text(sql), limit=conf['bulk_size']).fetchall()
        db_engine.dispose()

        start = time.time()

        if len(posts) == 0:
            time.sleep(0.5)
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

        end = time.time()
        logger.info('{} indexed in {}'.format(len(posts), (end - start)))

        post_ids = [x.post_id for x in posts]
        chunked_id_list = list(chunks(post_ids, 100))

        for chunk in chunked_id_list:
            sql = "DELETE FROM __h2e_posts WHERE post_id IN :ids"
            db_engine.execute(text(sql), ids=tuple(chunk))


def main():
    parser = configargparse.get_arg_parser()

    parser.add('--db-url', env_var='DB_URL', required=True, help='hive database connection url')
    parser.add('--es-url', env_var='ES_URL', required=True, help='elasticsearch connection url')
    parser.add('--es-index', env_var='ES_INDEX', help='elasticsearch index name', default='hive_posts')
    parser.add('--es-type', env_var='ES_TYPE', help='elasticsearch type name', default='posts')
    parser.add('--bulk-size', env_var='BULK_SIZE', type=int, help='number of records in a single loop', default=500)
    parser.add('--max-workers', type=int, env_var='MAX_WORKERS', help='max workers', default=2)
    parser.add('--max-bulk-errors', type=int, env_var='MAX_BULK_ERRORS', help='', default=5)

    args = parser.parse_args()

    global conf

    conf = vars(args)

    run()


if __name__ == "__main__":
    main()
