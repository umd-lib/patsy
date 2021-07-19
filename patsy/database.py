import argparse
import sys

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from .model import Base

Session = sessionmaker()


def use_database_file(database: str) -> None:
    # Set up database file or use in-memory db
    if database == ":memory:":
        sys.stderr.write(f"Using a transient in-memory database...")
        db_path = f"sqlite:///{database}"

    elif database.startswith('postgresql:'):
        sys.stderr.write(f"Using postgres database at {database}")
        db_path = database

    else:
        sys.stderr.write(f"Using database at {database}...")
        db_path = f"sqlite:///{database}"

    sys.stderr.write("Binding the database session...")

    engine = create_engine(db_path)

    # Enable foreign key constraints
    if not db_path.startswith('postgresql:'):
        event.listen(engine, 'connect',
                     lambda dbapi_con, con_record:
                         dbapi_con.execute('pragma foreign_keys=ON'))

    Session.configure(bind=engine)
