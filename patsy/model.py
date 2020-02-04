from collections import namedtuple
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

AccessionRecord = namedtuple('AccessionRecord', 
    "sourcefile sourceline filename bytes timestamp md5"
    )

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
    __tablename__ = "instances"

    id = Column(Integer, primary_key=True)
    filename = Column(String)
    dirlist_id = Column(Integer, ForeignKey("dirlists.id"))

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
    __tablename__ = "dirlists"

    id = Column(Integer, primary_key=True)
    filename = Column(String)
    md5 = Column(Integer)
    bytes = Column(Integer)
    batch_id = Column(Integer, ForeignKey("batches.id"))

    batch = relationship("Batch", back_populates="dirlists")

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
    __tablename__ = "assets"

    id = Column(Integer, primary_key=True)
    filename = Column(String)
    timestamp = Column(String)
    md5 = Column(String)
    bytes = Column(Integer)

    dirlist_id = Column(Integer, ForeignKey("dirlists.id"))
    dirlist_line = Column(Integer)
    dirlist = relationship("Dirlist", back_populates="assets")

    def __repr__(self):
        return f"<Asset(name='{self.filename}', bytes={self.bytes}, md5='{self.md5}')>"


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

    dirlist = relationship("Dirlist", back_populates="batch")

    def __repr__(self):
        return f"<Batch(name='{self.name}'>"


Batch.dirlists = relationship(
    "Dirlist", order_by=Dirlist.id, back_populates="batch"
    )
Dirlist.assets = relationship(
    "Asset", order_by=Asset.id, back_populates="dirlist"
    )
