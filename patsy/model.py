
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

    uuid = Column(Integer, primary_key=True)
    filename = Column(String)

    def __repr__(self):
        return f"<Instances(name='{self.filename}'>")


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
        return f"<Dirlist(filename='{self.filename}'>")


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
    bytes = Column(integer)

    def __repr__(self):
        return f"<Asset(name='{self.name}', bytes=({self.bytes})>")


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
        return f"<Batch(name='{self.name}'>")
    