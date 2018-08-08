import os
import pickle

__state_file = os.path.join(os.path.expanduser('~'), 'hive2elastic.state')


def get_state():
    try:
        with open(__state_file, 'rb') as f:
            b = pickle.load(f)
            f.close()
    except FileNotFoundError:
        return None

    return b


def set_state(index, last_post_id):
    d = {'index': index, 'last_post_id': last_post_id}

    with open(__state_file, 'wb') as f:
        pickle.dump(d, f, protocol=pickle.HIGHEST_PROTOCOL)
        f.close()


def delete_state():
    os.remove(__state_file)
