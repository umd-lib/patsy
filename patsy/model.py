from sqlalchemy import Column, Integer, String, Index, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


# Many-to-many relationship between accessions and restores that are perfect matches
perfect_matches_table = Table('perfect_matches', Base.metadata,
                              Column('accession_id', Integer, ForeignKey('accessions.id')),
                              Column('restore_id', Integer, ForeignKey('restores.id'))
                              )

Index('perfect_matches_accession_id', perfect_matches_table.c.accession_id, unique=False)
Index('perfect_matches_restore_id', perfect_matches_table.c.restore_id, unique=False)


# Many-to-many relationship between accessions and restores where filename and bytes
# are the same, but the MD5 checksum is different
altered_md5_matches_table = Table('altered_md5_matches', Base.metadata,
                                  Column('accession_id', Integer, ForeignKey('accessions.id')),
                                  Column('restore_id', Integer, ForeignKey('restores.id'))
                                  )

# Many-to-many relationship between accessions and restores where the filename
# is the same, but the MD5 checksum and bytes are different
filename_only_matches_table = Table('filename_only_matches', Base.metadata,
                                    Column('accession_id', Integer, ForeignKey('accessions.id')),
                                    Column('restore_id', Integer, ForeignKey('restores.id'))
                                    )


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
    perfect_matches = relationship("Restore", secondary=perfect_matches_table, back_populates="perfect_matches")
    altered_md5_matches = relationship("Restore", secondary=altered_md5_matches_table, back_populates="altered_md5_matches")
    filename_only_matches = relationship("Restore", secondary=filename_only_matches_table,
                                         back_populates="filename_only_matches")

    def __repr__(self):
        return f"<Accession(id='{self.id}', batch='{self.batch}', relpath='{self.relpath}'>"


Index('accession_batch_relpath', Accession.batch, Accession.relpath, unique=True)
Index('accession_md5', Accession.md5, unique=False)
Index('accession_batch', Accession.batch, unique=False)


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
    perfect_matches = relationship("Accession", secondary=perfect_matches_table, back_populates="perfect_matches")
    altered_md5_matches = relationship("Accession", secondary=altered_md5_matches_table, back_populates="altered_md5_matches")
    filename_only_matches = relationship("Accession", secondary=filename_only_matches_table,
                                         back_populates="filename_only_matches")
    transfers = relationship("Transfer", back_populates="restore")

    def __repr__(self):
        return f"<Restore(id='{self.id}', filepath='{self.filepath}'>"


Index('restore_filepath', Restore.filepath, unique=True)
Index('restore_md5', Restore.md5, unique=False)


class Transfer(Base):
    """
    Class representing a transfer record listing
    """

    __tablename__ = "transfers"

    id = Column(Integer, primary_key=True)
    filepath = Column(String)
    storagepath = Column(String)
    restore_id = Column(Integer, ForeignKey('restores.id'))
    restore = relationship("Restore", back_populates="transfers")

    def __repr__(self):
        return f"<Transfer(id='{self.id}', filepath='{self.filepath}', storagepath='{self.storagepath}'>"


Index('transfer_filepath_storagepath', Transfer.filepath, Transfer.storagepath, unique=True)
Index('transfer_filepath', Transfer.filepath, unique=False)
Index('transfer_restore_id', Transfer.restore_id, unique=False)
