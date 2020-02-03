from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Instance(Base):
    """
    CREATE TABLE instances(
        uuid         TEXT PRIMARY KEY UNIQUE NOT NULL,
        filename     TEXT,
        md5          TEXT,
        bytes        INTEGER,
        dirlist_id   INTEGER,
        dirlist_line INTEGER,
        path         TEXT,
        action       TEXT,
        FOREIGN KEY(dirlist_id) REFERENCES dirlists(id)
    );
    """
    __tablename__ = 'instances'

    id = Column(Integer, primary_key=True)
    filename = Column(String)

    def __repr__(self):
        return f"<Instances(name='{self.filename}'>"


class Dirlist(Base):
    """
    CREATE TABLE dirlists(
        id          INTEGER PRIMARY KEY UNIQUE NOT NULL,
        filename    TEXT,
        md5         TEXT,
        bytes       INTEGER,
        batch_id    INTEGER,
        FOREIGN KEY(batch_id) REFERENCES batches(id)
    );
    """
    __tablename__ = 'dirlists'

    id = Column(Integer, primary_key=True)
    filename = Column(String)
    md5 = Column(Integer)
    bytes = Column(Integer)

    def __repr__(self):
        return f"<Dirlist(filename='{self.filename}'>"


class Asset(Base):
    """
    CREATE TABLE assets(
        id          INTEGER PRIMARY KEY UNIQUE NOT NULL,
        filename    TEXT,
        md5         TEXT,
        bytes       INTEGER,
        dirlist_id   INTEGER,
        dirlist_line INTEGER,
        relpath     TEXT,
        FOREIGN KEY(dirlist_id) REFERENCES dirlists(id)
    );
    """
    __tablename__ = 'assets'

    id = Column(Integer, primary_key=True)
    md5 = Column(String)
    bytes = Column(Integer)

    def __repr__(self):
        return f"<Asset(name='{self.name}', bytes=({self.bytes})>"


class Batch(Base):
    """
    CREATE TABLE batches(
    id          INTEGER PRIMARY KEY UNIQUE NOT NULL,
    name        TEXT
    );
    """
    __tablename__ = 'batches'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        return f"<Batch(name='{self.name}'>"


Asset.instances = relationship(
    "Instances", order_by=Instance.id, back_populates="instance"
    )
Instance.dirlist = relationship(
    "Dirlists", order_by=Dirlist.id, back_populates="dirlist"
    )
Dirlist.batch = relationship(
    "Batches", order_by=batch.id, back_populates="batch"
    )


'''
class Asset():

    def __init__(self, 
                 filename,
                 md5=None,
                 bytes=None,
                 timestamp=None,
                 source_id, 
                 source_line,
                 relpath=None
                 ):
        self.filename = filename
        self.bytes = bytes
        self.timestamp = timestamp
        self.md5 = md5
        self.restored = []
        self.extra_copies = []
        self.sourcefile = sourcefile
        self.sourceline = sourceline
        self.status = 'Not checked'

    @property
    def signature(self):
        return (self.filename, self.md5, self.bytes)


class RestoredAsset():

    def __init__(self, id, bytes, md5, filename, path):
        self.id = id
        self.bytes = bytes
        self.md5 = md5
        self.filename = filename
        self.path = path


class Batch():

    def __init__(self, identifier, *dirlists):
        self.identifier = identifier
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
'''

