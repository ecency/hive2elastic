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

    track_table = conf['db_track_table']

    try:
        db_engine = create_engine(conf['db_url'])
        sql = 'SELECT author, permlink FROM {} LIMIT 1'.format(track_table)
        db_engine.execute(sql)
    except OperationalError:
        raise Exception("Could not connected: {}".format(conf['db_url']))
    except ProgrammingError:
        raise Exception("{} table not exists in database".format(track_table))

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
        start = time.time()

        # Two-step pull: first claim the keys we'll work on, then fetch the
        # rows. Lets us DELETE-what-we-selected unconditionally below, so
        # queue entries that no longer have a matching hive_posts_raw row
        # (stale post_id from an older schema, manual cleanup, etc.) can't
        # head-block the queue. The old code did SELECT-with-JOIN and only
        # DELETEd if `len(posts) > 0`, which left orphans cycling forever.
        select_keys_sql = (
            'SELECT author, permlink FROM {} '
            'ORDER BY author, permlink LIMIT :limit'
        ).format(track_table)
        key_rows = db_engine.execute(
            text(select_keys_sql), limit=conf['bulk_size']
        ).fetchall()

        if len(key_rows) == 0:
            time.sleep(0.5)
            continue

        keys = [(r.author, r.permlink) for r in key_rows]

        # Resolve to full rows. Composite-key matching via row constructor
        # `(author, permlink) IN ((:a1, :p1), ...)`. SQLAlchemy's
        # `expanding`-style parameter for tuples needs a manually-built
        # placeholder list because tuple-element binds aren't first-class.
        placeholders = ', '.join(
            '(:a{i}, :p{i})'.format(i=i) for i in range(len(keys))
        )
        params = {}
        for i, (a, p) in enumerate(keys):
            params['a{}'.format(i)] = a
            params['p{}'.format(i)] = p
        fetch_sql = '''SELECT
              post_id,
              author,
              permlink,
              category,
              depth,
              children,
              author_rep,
              total_votes,
              up_votes,
              title,
              img_url,
              payout,
              pending_payout,
              promoted,
              created_at,
              payout_at,
              updated_at,
              is_paidout,
              is_nsfw,
              is_declined,
              is_full_power,
              is_hidden,
              is_grayed,
              rshares,
              abs_rshares,
              sc_hot,
              sc_trend,
              body,
              0 AS votes,
              json
              FROM hive_posts_raw
              WHERE (author, permlink) IN ({})'''.format(placeholders)
        posts = db_engine.execute(text(fetch_sql), **params).fetchall()
        db_engine.dispose()

        if len(posts) > 0:
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

        # Delete the keys we *selected*, not the keys we *processed*. This
        # is what keeps stale queue entries from head-blocking forever.
        chunked_keys = list(chunks(keys, 200))
        for chunk in chunked_keys:
            del_placeholders = ', '.join(
                '(:a{i}, :p{i})'.format(i=i) for i in range(len(chunk))
            )
            del_params = {}
            for i, (a, p) in enumerate(chunk):
                del_params['a{}'.format(i)] = a
                del_params['p{}'.format(i)] = p
            del_sql = 'DELETE FROM {} WHERE (author, permlink) IN ({})'.format(
                track_table, del_placeholders
            )
            db_engine.execute(text(del_sql), **del_params)

        end = time.time()
        logger.info('{} selected / {} indexed in {}'.format(
            len(keys), len(posts), (end - start)
        ))


def main():
    parser = configargparse.get_arg_parser()

    parser.add('--db-url', env_var='DB_URL', required=True, help='hive database connection url')
    parser.add('--db-track-table', env_var='DB_TRACK_TABLE', help='db table to track updates', default='__h2e_posts')
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
