from sqlalchemy import Column, Integer, String, Index, ForeignKey, Table, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()

# Many-to-many relationship between accessions and locations
accession_locations_table = Table('accession_locations', Base.metadata,
                                  Column('accession_id', Integer, ForeignKey('accessions.id', ondelete='CASCADE')),
                                  Column('location_id', Integer, ForeignKey('locations.id')))

Index('accession_locations_accession_id', accession_locations_table.c.accession_id, unique=False)
Index('accession_locations_location_id', accession_locations_table.c.location_id, unique=False)


class Batch(Base):
    """
    Class representing a batch
    """

    __tablename__ = "batches"

    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        return f"<Batch(id='{self.id}', name='{self.name}'>"


class Accession(Base):
    """
    Class representing an authoritative accession record listing
    """

    __tablename__ = "accessions"

    id = Column(Integer, primary_key=True)
    batch_id = Column(Integer, ForeignKey('batches.id'))
    relpath = Column(String)
    filename = Column(String)
    extension = Column(String)
    bytes = Column(BigInteger)
    timestamp = Column(String)
    md5 = Column(String)
    sha1 = Column(String)
    sha256 = Column(String)

    batch = relationship("Batch", back_populates="accessions")
    locations = relationship(
        "Location", secondary=accession_locations_table, back_populates="accessions")

    def __repr__(self):
        return f"<Accession(id='{self.id}', batch='{self.batch}', relpath='{self.relpath}'>"


Batch.accessions = relationship("Accession", order_by=Accession.id, back_populates="batch")

Index('batch_name', Batch.name)
Index('accession_batch_relpath', Accession.batch_id, Accession.relpath, unique=True)


class Location(Base):
    """
    Class representing a storage location for an accession.
    """

    __tablename__ = "locations"

    id = Column(Integer, primary_key=True)
    storage_provider = Column(String)
    storage_location = Column(String)
    accessions = relationship("Accession", secondary=accession_locations_table, back_populates="locations")

    def __repr__(self):
        return f"<Location(id='{self.id}', storage_provider='{self.storage_provider}', " \
               f"storage_location='{self.storage_location}'>"


Index('location_storage', Location.storage_provider, Location.storage_location, unique=True)
