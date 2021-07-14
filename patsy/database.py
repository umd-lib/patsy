import argparse
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from .model import Base

Session = sessionmaker()


def use_database_file(database: str) -> None:
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


def create_schema(args: argparse.Namespace) -> None:
    use_database_file(args.database)
    session = Session()
    engine = session.get_bind()
    print("Creating the schema using the declarative base...")
    Base.metadata.create_all(engine)

    # Create "patsy_record" view
    with engine.connect() as con:
        con.execute("DROP VIEW IF EXISTS patsy_record;")
        rs = con.execute("""
            CREATE VIEW patsy_record AS
                SELECT
                    batches.id as "batch_id",
                    batches.name as "batch_name",
                    accessions.id as "accession_id",
                    accessions.relpath,
                    accessions.filename,
                    accessions.extension,
                    accessions.bytes,
                    accessions.timestamp,
                    accessions.md5,
                    accessions.sha1,
                    accessions.sha256,
                    locations.id as "location_id",
                    locations.storage_provider,
                    locations.storage_location
                    FROM batches
                    LEFT JOIN accessions ON batches.id = accessions.batch_id
                    LEFT JOIN accession_locations ON accessions.id = accession_locations.accession_id
                    LEFT JOIN locations ON accession_locations.location_id = locations.id
                    ORDER BY batches.id
        """)
