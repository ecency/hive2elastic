import json
import re
from datetime import datetime
import markdown2
import timeout_decorator

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
                        'filter': ['standard', 'lowercase', 'asciifolding', 'my_word_delimiter']
                    }
                },
                'filter': {
                    'my_word_delimiter': {
                        'type': 'word_delimiter',
                        'split_on_numerics': 'false',
                        'generate_number_parts': 'false',
                        'preserve_original': 'true',
                        'type_table': ['# => ALPHANUM', '@ => ALPHANUM']
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
                        'type': 'text',
                        'analyzer': 'whitespace'
                    },
                    'permlink': {
                        'type': 'text',
                        'index': 'false'
                    },
                    'category': {
                        'type': 'text',
                        'index': 'false'
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
                        'type': 'text'
                    },
                    'img_url': {
                        'type': 'text',
                        'index': 'false'
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
                        'type': 'text',
                        'index': 'false'
                    },
                    'body_sanitized': {
                        'type': 'text'
                    },
                    'votes': {
                        'type': 'text',
                        'index': 'false'
                    },
                    'tags': {
                        'type': 'text',
                        'analyzer': 'whitespace'
                    },
                    'app': {
                        'type': 'text',
                        'analyzer': 'whitespace'
                    },
                    'searchable': {
                        'type': 'text',
                        'analyzer': 'index_analyzer'
                    }
                }
            }
        }
    }


@timeout_decorator.timeout(5)
def sanitize_post_body(body):
    """

    >>> sanitize_post_body(None)
    ''

    >>> sanitize_post_body(11)
    ''

    >>> sanitize_post_body('#this is a h1')
    'this is a h1'

    >>> sanitize_post_body('```lorem ipsum dolor sit amet``` There are many variations of passages')
    'lorem ipsum dolor sit amet There are many variations of passages'

    >>> sanitize_post_body('<center>using [eSteem](https://play.google.com/store/apps/details?id=com.netsolutions.esteem)</center>')
    'using eSteem'

    >>> s = '![img](https://img.lorem.ip/11.jpg)[Source](https://images.unsplash.com/photo-1518898053858-dcb) Lorem ipsum dolor sit amet ![img2](https://img.lorem.ip/222.jpg) *foo* **bar** baz'
    >>> sanitize_post_body(s)
    'Source Lorem ipsum dolor sit amet foo bar baz'
    """

    if not isinstance(body, str):
        return ''

    html = markdown2.markdown(body)

    sanitized = re.sub(re.compile('<.*?>'), '', html).strip()

    while '  ' in sanitized:
        sanitized = sanitized.replace('  ', ' ')

    return sanitized


def parse_app(app):
    """

    >>> parse_app(None)
    ''

    >>> parse_app({})
    ''

    >>> parse_app({'name': 'esteem'})
    'esteem'

    >>> parse_app({'version': 2})
    ''

    >>> parse_app({'name': 'esteem', 'version': 2})
    'esteem 2'

    >>> parse_app('esteem 1.1.1')
    'esteem 1.1.1'

    """

    if app is None:
        return ''

    if isinstance(app, dict):
        if 'name' in app:
            ver = app['version'] if 'version' in app else ''
            return '{} {}'.format(app['name'], ver).strip()
        else:
            return ''

    return str(app)


def parse_tags(tags):
    """

    >>> parse_tags('writing')
    'writing'

    >>> parse_tags(['news', 'steemit', 3, {'5': {}, '3': {}, '1': {}}, {'39': {}, '45': {}, '11': {}}, {}, 'esteem'])
    ['news', 'steemit', 'esteem']

    >>> parse_tags(['dlive', 'dlive-broadcast', 'game', 'DLIVEGAMING'])
    ['dlive', 'dlive-broadcast', 'game', 'DLIVEGAMING']

    >>> parse_tags(2)
    ''
    """

    if isinstance(tags, str):
        return tags

    if isinstance(tags, list):
        return [x for x in tags if isinstance(x, str)]

    return ''


def doc_from_row(row, index_name, index_type):
    json_obj = json.loads(row.json)

    tags = parse_tags(json_obj['tags']) if 'tags' in json_obj else ''
    app = parse_app(json_obj['app']) if 'app' in json_obj else ''

    try:
        sanitized_body = sanitize_post_body(row.body)
    except RecursionError:
        sanitized_body = row.body
    except timeout_decorator.TimeoutError:
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
        'body': row.body,
        'body_sanitized': sanitized_body,
        'votes': row.votes,
        'tags': tags,
        'app': app,
        'searchable': '{} | {}'.format(row.title, sanitized_body)
    }
