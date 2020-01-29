#!/usr/bin/env python3

import collections
import csv
from datetime import datetime
import hashlib
import os
import re
import sys

FIELDS = ['sourcefile', 'sourceline', 'filename', 'bytes', 'timestamp', 'md5']
Asset = collections.namedtuple('Asset', ' '.join(FIELDS))


def calculate_md5(path):
    """
    Calclulate and return the object's md5 hash.
    """
    hash = hashlib.md5()
    with open(path, 'rb') as f:
        while True:
            data = f.read(8192)
            if not data:
                break
            else:
                hash.update(data)
    return hash.hexdigest()


def human_readable(bytes):
    """
    Return a human-readable representation of the provided number of bytes.
    """
    for n, label in enumerate(['bytes', 'KiB', 'MiB', 'GiB', 'TiB']):
        value = bytes / (1024 ** n)
        if value < 1024:
            return f'{round(value, 2)} {label}'
        else:
            continue


class DirList():

    def __init__(self, path):
        self.filename = os.path.basename(path)
        self.path = path
        self.bytes = int(os.path.getsize(path))
        self.md5 = calculate_md5(path)
        self.dirlines = 0
        self.extralines = 0
        self.lines = self.read()
        if self.lines[0].startswith('Volume in drive'):
            self.assets = self.parse_space_delimited_dirlist()
        elif ';' in self.lines[0]:
            self.assets = self.parse_semicolon_delimited_dirlist()
        else:
            self.assets = self.parse_tabular_formats()     

    def read(self):
        for encoding in ['utf8', 'iso-8859-1', 'macroman']:
            try:
                with open(self.path, encoding=encoding) as handle:
                    return [line.strip() for line in handle.readlines()]
            except ValueError:
                continue
        print(f'Could not read directory listing file {self.path}')
        sys.exit(1)

    def parse_space_delimited_dirlist(self):
        ptrn = r'^(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}\s[AP]M)\s+([0-9,]+)\s(.+?)$'
        for n, line in enumerate(self.lines, 1):
            # check if the line describes an asset
            match = re.match(ptrn, line)
            if not match:
                continue
            else:
                timestamp = datetime.strptime(match.group(1),
                                              '%m/%d/%Y %I:%M %p')
                bytes = int(''.join([c for c in match.group(2) if c.isdigit()]))
                filename = match.group(3)
                yield Asset(filename=filename, bytes=bytes,
                            timestamp=timestamp, sourcefile=self.filename,
                            sourceline=n, md5=None)

    def parse_semicolon_delimited_dirlist(self):
        for n, line in enumerate(self.lines, 1):
            cols = line.split(';')
            if cols[2] == 'Directory':
                continue
            else:
                filename = os.path.basename(cols[0].rsplit('\\')[-1])
                timestamp = datetime.strptime(cols[1], '%m/%d/%Y %I:%M:%S %p')
                bytes = round(float(cols[2].replace(',', '')) * 1024)
                yield Asset(filename=filename, bytes=bytes, timestamp=timestamp,     
                            sourcefile=self.filename, sourceline=n, md5=None)

    def parse_tabular_formats(self):
        delimiter = '\t' if '\t' in self.lines[0] else ','
        possible_keys = {'filename': ['Filename', 'File Name', 'FILENAME',             
                                      'Key', '"Filename"', '"Key"'],
                         'bytes': ['Size', 'SIZE', 'File Size', 'Bytes', 
                                   'BYTES', '"Size"'],
                         'timestamp': ['Mod Date', 'Moddate', 'MODDATE', 
                                       '"Mod Date"'],
                         'md5': ['MD5', 'Other', 'Data', '"Other"', 
                                 '"Data"', 'md5']
                        }
        columns = self.lines[0].split(delimiter)
        operative_keys = {}
        for attribute, keys in possible_keys.items():
            for key in keys:
                if key in columns:
                    operative_keys[attribute] = key.replace('"','')
                    break
        reader = csv.DictReader(self.lines, quotechar='"', delimiter=delimiter)
        for n, row in enumerate(reader, 1):
            # Skip extra rows in Prange-style "CSV" files
            if 'File Name' in row and any([
                (row.get('Type') == 'Directory'),
                (row.get('File Name').startswith('Extension')),
                (row.get('File Name').startswith('Total file size')),
                (row.get('File Name') == '')
                ]):
                continue
            else:
                filename_key = operative_keys.get('filename')
                if filename_key is not None:
                    filename = row[filename_key]
                else:
                    filename = None

                bytes_key = operative_keys.get('bytes')
                if bytes_key is not None:
                    raw = row[bytes_key]
                    digits = ''.join([c for c in raw if c.isdigit()])
                    if digits is not '':
                        bytes = int(digits)
                    else:
                        bytes = None
                else:
                    bytes = None

                timestamp_key = operative_keys.get('timestamp')
                if timestamp_key is not None:
                    timestamp = row[timestamp_key]
                else:
                    timestamp = None

                md5_key = operative_keys.get('md5')
                if md5_key is not None:
                    md5 = row[md5_key]
                else:
                    md5 = None
                yield Asset(filename=filename, bytes=bytes,
                            timestamp=timestamp, md5=md5,
                            sourcefile=self.filename, sourceline=n)


def main():
    sourcedir = sys.argv[1]
    outputfile = sys.argv[2]
    
    with open(outputfile, 'w') as handle:
        writer = csv.writer(handle)
        for sourcefile in sorted(os.listdir(sourcedir)):
            dirlist = DirList(os.path.join(sourcedir, sourcefile))
            for asset in dirlist.assets:
                writer.writerow(asset)


if __name__ == "__main__":
    main()

