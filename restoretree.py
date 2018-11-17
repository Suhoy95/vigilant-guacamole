import os
import json
import hashlib

from os.path import (
    join,
    normpath,
    basename,
    dirname,
    isfile,
    isdir,
    getsize
)

import nameserver

from nameserver import Dir, File

def md5(filename):
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def walk(absroot, curdir, tree):
    d = normpath(join(absroot, '.' + curdir))

    for filename in os.listdir(d):
        fullpath = normpath(join(d, filename))
        dfs_path = normpath(join(curdir, filename))

        tree[curdir]['files'].append(filename)

        if isfile(fullpath):
            tree[dfs_path] = File(dfs_path, getsize(fullpath))
            tree[dfs_path]['md5sum'] = md5(fullpath)
        elif isdir(fullpath):
            tree[dfs_path] = Dir(dfs_path, [])
            walk(absroot, dfs_path, tree)
        else:
            raise ValueError("Bad file {}".format(fullpath))


def restoreTree(storageRoot):
    tree = {
        '/': Dir('/', [])
    }

    absroot = os.path.abspath(storageRoot)

    walk(absroot, '/', tree)

    return tree



def restoretree(storageRoot, outfile):
    tree = restoreTree(args.root)
    with open(args.out, "w") as f:
        f.write(json.dumps(tree, indent=4))

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("storageRoot")
    parser.add_argument("outfile")

    args = parser.parse_args()

    restoretree(args.storageRoot, args.outfile)







