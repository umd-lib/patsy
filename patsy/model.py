from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


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
        return (f"<Asset(name='{self.filename}', ",
                f"bytes={self.bytes}, md5='{self.md5}')>")


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


class RestoredFile(Base):
    """
    Class representing a temporary instance restored from tape backup
    """

    __tablename__ = "restores"

    id = Column(Integer, primary_key=True)
    filename = Column(String)
    md5 = Column(String)
    bytes = Column(Integer)
    path = Column(String)
    relpath = Column(String)
    action = Column(String)

    def __repr__(self):
        return f"<RestoredFile(id='{self.id}', name='{self.filename}'>"



Batch.dirlists = relationship(
    "Dirlist", order_by=Dirlist.id, back_populates="batch"
    )
Dirlist.assets = relationship(
    "Asset", order_by=Asset.id, back_populates="dirlist"
    )
