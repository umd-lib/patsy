from sqlalchemy import Column, Integer, String, Index, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


class Accession(Base):
    """
    Class representing an authoritative accession record listing
    """

    __tablename__ = "accessions"

    id = Column(Integer, primary_key=True)
    batch = Column(String)
    sourcefile = Column(String)
    sourceline = Column(Integer)
    filename = Column(String)
    bytes = Column(Integer)
    timestamp = Column(String)
    relpath = Column(String)
    md5 = Column(String)

    def __repr__(self):
        return f"<Accession(id='{self.id}', batch='{self.batch}', relpath='{self.relpath}'>"


Index('accession_batch_index', Accession.batch, Accession.relpath, unique=True)


class Restore(Base):
    """
    Class representing a restore record listing
    """

    __tablename__ = "restores"

    id = Column(Integer, primary_key=True)
    md5 = Column(String)
    filename = Column(String)
    filepath = Column(String)
    bytes = Column(Integer)

    def __repr__(self):
        return f"<Restore(id='{self.id}', filepath='{self.filepath}'>"


Index('restore_filepath', Restore.filepath, unique=True)


class Batch(Base):
    """
    Class representing a group of assets in a content stream
    """

    __tablename__ = 'batches'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        return f"<Batch(id='{self.id}', name='{self.name}'>"


class Dirlist(Base):
    """
    Class representing an authoritative accession record listing
    """

    __tablename__ = "dirlists"

    id = Column(Integer, primary_key=True)
    filename = Column(String)
    md5 = Column(String)
    bytes = Column(Integer)

    batch_id = Column(Integer, ForeignKey("batches.id"))
    batch = relationship("Batch", back_populates="dirlists")


    def __repr__(self):
        return f"<Dirlist(id='{self.id}', filename='{self.filename}'>"


class Asset(Base):
    """
    Class representing a digital asset under preservation
    """

    __tablename__ = "assets"

    id = Column(Integer, primary_key=True)
    filename = Column(String)
    timestamp = Column(String)
    md5 = Column(String)
    bytes = Column(Integer)
    dirlist_line = Column(Integer)

    dirlist_id = Column(Integer, ForeignKey("dirlists.id"))
    dirlist = relationship("Dirlist", back_populates="assets")

    def __repr__(self):
        return f"<Asset(name='{self.filename}', " + \
               f"bytes={self.bytes}, md5='{self.md5}')>"


class Instance(Base):
    """
    Class representing a copy of a file under preservation
    """

    __tablename__ = "instances"

    id = Column(Integer, primary_key=True)
    filename = Column(String)
    md5 = Column(String)
    bytes = Column(Integer)
    path = Column(String)
    action = Column(String)

    asset_id = Column(Integer, ForeignKey("assets.id"))

    def __repr__(self):
        return f"<Instance(id='{self.id}', name='{self.filename}'>"


class RestoredFileList(Base):

    __tablename__ = "restoredfilelists"

    id = Column(Integer, primary_key=True)
    filename = Column(String)
    md5 = Column(String)
    bytes = Column(Integer)
    commonroot = Column(String)

    def __repr__(self):
        return f"<RestoredFileList(id='{self.id}', name='{self.filename}'>"


Batch.dirlists = relationship(
    "Dirlist", order_by=Dirlist.id, back_populates="batch"
    )
Dirlist.assets = relationship(
    "Asset", order_by=Asset.id, back_populates="dirlist"
    )
