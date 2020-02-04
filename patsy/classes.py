import os

from .utils import calculate_md5

class DirList():

    def __init__(self, path):
        self.path = path
        self.filename = os.path.basename(self.path)
        self.bytes = int(os.path.getsize(self.path))
        self.md5 = calculate_md5(self.path)
        self.lines = self.read()

    def read(self):
        """Read dirlist file by trying different encodings"""
        for encoding in ['utf8', 'iso-8859-1', 'macroman']:
            try:
                with open(self.path, encoding=encoding) as handle:
                    return [line.strip() for line in handle.readlines()]
            except ValueError:
                continue
        print(f'Could not read directory listing file {self.path}')
        sys.exit(1)


class Batch():

    def __init__(self, name, *dirlists):
        self.name = identifier
        self.dirlists = [d for d in dirlists]
        self.assets = []
        self.status = None
        for dirlist in self.dirlists:
            self.load_assets(dirlist)

    @property
    def bytes(self):
        return sum(
            [asset.bytes for asset in self.assets if asset.bytes is not None]
            )

    @property
    def has_hashes(self):
        return all(
            [asset.md5 is not None for asset in self.assets]
            )

    def load_assets(self, dirlist):
        self.assets.extend([asset for asset in dirlist.assets])

    def summary_dict(self):
        return {'identifier': self.identifier,
                'dirlists': {d.md5: d.filename for d in self.dirlists},
                'num_assets': len(self.assets),
                'bytes': self.bytes,
                'human_readable': human_readable(self.bytes),
                'status': self.status
                }
    @property
    def asset_root(self):
        return os.path.commonpath([a.restored.path for a in self.assets])

    def has_duplicates(self):
        return len(self.assets) < len(set([a.signature for a in self.assets]))
