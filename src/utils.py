import json

import src.proto as proto


def load_tree(filename):
    with open(filename, 'r') as f:
        return json.load(f)


def dump_tree(tree, filename):
    # TODO: write to tmpfile -> rename
    with open(filename, "w") as f:
        f.write(json.dumps(tree, indent=4))


def Dir(path, files=[]):
    return {
        'type': proto.DIRECTORY,
        'size': 0,
        'path': path,
        'files': files
    }


def File(path, size):
    return {
        'type': proto.FILE,
        'size': size,
        'path': path,
        'nodes': [
            ('localhost', 8084)
        ]
    }
