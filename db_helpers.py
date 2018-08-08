from sqlalchemy import create_engine, text


def get_source_data(db_url, bulk_size, min_post_id=0):
    db_engine = create_engine(db_url)

    sql = """SELECT post_id, author, permlink, category, depth, children,
             author_rep, flag_weight, total_votes, up_votes, title, img_url, payout, promoted, 
             created_at, payout_at, updated_at, is_paidout, is_nsfw, is_declined, is_full_power, is_hidden, is_grayed,
             rshares, sc_hot, sc_trend, sc_hot, body, votes, json FROM hive_posts_cache
             WHERE post_id > :min_post_id
             ORDER BY post_id ASC LIMIT :limit"""

    posts = db_engine.execute(text(sql), min_post_id=min_post_id, limit=bulk_size).fetchall()

    db_engine.dispose()

    return posts


def check_conn(db_url):
    try:
        db_engine = create_engine(db_url)
        db_engine.execute("SELECT 1")
        db_engine.dispose()
        return True
    except Exception:
        return False
