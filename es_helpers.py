import json
import re
from datetime import datetime

import mistune

markdown = mistune.Markdown()

max_post_id_agg = {
    "aggs": {
        "max_post_id": {
            "max": {
                "field": "post_id"
            }
        }
    },
    "size": 0
}


def make_new_index_name(base_index_name):
    return '{}-{}'.format(base_index_name, datetime.utcnow().strftime('%Y-%m-%dt%H:%M:%S'))


def make_index_config(type_name):
    return {
        'settings': {
            'analysis': {
                'analyzer': {
                    'index_analyzer': {
                        'tokenizer': 'whitespace',
                        'filter': ['standard', 'lowercase', 'asciifolding']
                    }
                }
            }
        },
        'mappings': {
            type_name: {
                '_all': {'enabled': False},
                'properties': {
                    'post_id': {
                        'type': 'integer'
                    },
                    'author': {
                        'type': 'text'
                    },
                    'permlink': {
                        'type': 'text'
                    },
                    'category': {
                        'type': 'text'
                    },
                    'depth': {
                        'type': 'short'
                    },
                    'children': {
                        'type': 'integer'
                    },
                    'author_rep': {
                        'type': 'float'
                    },
                    'flag_weight': {
                        'type': 'float'
                    },
                    'total_votes': {
                        'type': 'integer'
                    },
                    'up_votes': {
                        'type': 'integer'
                    },
                    'title': {
                        'type': 'text',
                        'analyzer': 'index_analyzer'
                    },
                    'img_url': {
                        'type': 'text'
                    },
                    'payout': {
                        'type': 'float'
                    },
                    'promoted': {
                        'type': 'float'
                    },
                    'created_at': {
                        'type': 'date'
                    },
                    'payout_at': {
                        'type': 'date'
                    },
                    'updated_at': {
                        'type': 'date'
                    },
                    'is_paidout': {
                        'type': 'boolean'
                    },
                    'is_nsfw': {
                        'type': 'boolean'
                    },
                    'is_declined': {
                        'type': 'boolean'
                    },
                    'is_full_power': {
                        'type': 'boolean'
                    },
                    'is_hidden': {
                        'type': 'boolean'
                    },
                    'is_grayed': {
                        'type': 'boolean'
                    },
                    'rshares': {
                        'type': 'long'
                    },
                    'sc_trend': {
                        'type': 'float'
                    },
                    'sc_hot': {
                        'type': 'float'
                    },
                    'body': {
                        'type': 'text'
                    },
                    'body_sanitized': {
                        'type': 'text',
                        'analyzer': 'index_analyzer'
                    },
                    'votes': {
                        'type': 'text'
                    },
                    'tags': {
                        'type': 'text'
                    },
                    'app': {
                        'type': 'text'
                    }
                }
            }
        }
    }


def sanitize_post_body(body):
    html = markdown(body)

    return re.sub(re.compile('<.*?>'), '', html)


def parse_app(app):
    if isinstance(app, dict):
        name = app['name'] if 'name' in app else ''
        ver = app['version'] if 'version' in app else ''
        return '{} {}'.format(name, ver).strip()

    return app


def parse_tags(tags):
    """

    >>> parse_tags('writing')
    'writing'

    >>> parse_tags(['news', 'steemit', 3, {'5': {}, '3': {}, '1': {}}, {'39': {}, '45': {}, '11': {}}, {}, 'esteem'])
    ['news', 'steemit', 'esteem']

    >>> parse_tags(['dlive', 'dlive-broadcast', 'game', 'DLIVEGAMING'])
    ['dlive', 'dlive-broadcast', 'game', 'DLIVEGAMING']
    """

    if isinstance(tags, str):
        return tags

    return [x for x in tags if isinstance(x, str)]


def doc_from_row(row, index_name, index_type):
    json_obj = json.loads(row.json)

    tags = parse_tags(json_obj['tags']) if 'tags' in json_obj else ''
    app = parse_app(json_obj['app']) if 'app' in json_obj else ''

    try:
        sanitized_body = sanitize_post_body(row.body)
    except RecursionError:
        sanitized_body = row.body

    return {
        '_index': index_name,
        '_type': index_type,
        '_id': row.post_id,
        'post_id': row.post_id,
        'author': row.author,
        'permlink': row.permlink,
        'category': row.category,
        'depth': row.depth,
        'children': row.children,
        'author_rep': row.author_rep,
        'flag_weight': row.flag_weight,
        'total_votes': row.total_votes,
        'up_votes': row.up_votes,
        'title': row.title,
        'img_url': row.img_url,
        'payout': row.payout,
        'promoted': row.promoted,
        'created_at': row.created_at,
        'payout_at': row.payout_at,
        'updated_at': row.updated_at,
        'is_nsfw': row.is_nsfw,
        'is_declined': row.is_declined,
        'is_full_power': row.is_full_power,
        'is_hidden': row.is_hidden,
        'is_grayed': row.is_grayed,
        'rshares': row.rshares,
        'sc_trend': row.sc_trend,
        'sc_hot': row.sc_hot,
        'body': row.body,
        'body_sanitized': sanitized_body,
        'votes': row.votes,
        'tags': tags,
        'app': app
    }
