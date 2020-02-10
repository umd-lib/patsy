#!/usr/bin/env python3

import csv
import os
import sys

ROOT = sys.argv[1]
OUTPUT = sys.argv[2]


def common_root(path):
    with open(path) as handle:
        reader = csv.reader(handle)
        all_paths = [row[1] for row in reader]
        if all_paths:
            return os.path.commonpath(all_paths)
        else:
            return None


def main():

    results = []

    for n, filename in enumerate(os.listdir(ROOT), 1):
        path = os.path.join(ROOT, filename)
        commonpath = common_root(path) or ''
        results.append((str(n), filename, commonpath))
        print(f"{n} {filename} {commonpath}")
        with open(path) as handle:
            reader = csv.reader(handle)
            for n, row in enumerate(reader):
                md5 = row[0]
                path = row[1]
                filename = row[2]
                bytes = row[3]
                if path.startswith(commonpath):
                    relpath = path[len(commonpath):].lstrip('/')
                print(n, relpath)

    for result in results:
        print('\t'.join(result))

if __name__ == "__main__":
    main()