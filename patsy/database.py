from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from .model import Base

Session = sessionmaker()


def use_database_file(database):
    # Set up database file or use in-memory db
    if database == ":memory:":
        print(f"Using a transient in-memory database...")
        db_path = f"sqlite:///{database}"

    elif database.startswith('postgresql:'):
        print(f"Using postgres database at {database}")
        db_path = database

    else:
        print(f"Using database at {database}...")
        db_path = f"sqlite:///{database}"

    print("Binding the database session...")

    engine = create_engine(db_path)

    # Enable foreign key constraints
    if not db_path.startswith('postgresql:'):
        event.listen(engine, 'connect',
                     lambda dbapi_con, con_record:
                        dbapi_con.execute('pragma foreign_keys=ON'))

    Session.configure(bind=engine)


def create_schema(args):
    use_database_file(args.database)
    session = Session()
    engine = session.get_bind()
    print("Creating the schema using the declarative base...")
    Base.metadata.create_all(engine)

    # Create view of transferred records
    with engine.connect() as con:
        con.execute("DROP VIEW IF EXISTS transferred_inventory_records;")
        rs = con.execute("""
            CREATE VIEW transferred_inventory_records AS
            SELECT
                accessions.batch as "BATCH",
                restores.filepath as "PATH",
                accessions.relpath as "RELPATH",
                accessions.filename as "FILENAME",
                accessions.bytes as "BYTES",
                accessions.timestamp as "timestamp",
                accessions.md5 as "MD5",
                transfers.storagepath
            FROM accessions, restores, transfers, perfect_matches
            WHERE
                transfers.restore_id = restores.id AND
                accessions.id = perfect_matches.accession_id AND
                perfect_matches.restore_id = restores.id
            ORDER BY accessions.batch, accessions.relpath
        """)

    # Create view of untransferred records
    with engine.connect() as con:
        con.execute("DROP VIEW IF EXISTS untransferred_inventory_records;")
        rs = con.execute("""
            CREATE VIEW untransferred_inventory_records AS
            SELECT
              accessions.batch as "BATCH",
              "" as "PATH",
              accessions.relpath as "RELPATH",
              accessions.filename as "FILENAME",
              accessions.bytes as "BYTES",
              accessions.timestamp as "timestamp",
              accessions.md5 as "MD5",
              "" as "storagepath"
            FROM accessions
            LEFT JOIN perfect_matches ON accessions.id = perfect_matches.accession_id
            WHERE perfect_matches.restore_id is NULL;
        """)
