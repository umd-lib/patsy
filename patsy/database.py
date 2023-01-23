import sys
import os

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker


Session = sessionmaker()


class DatabaseNotSetError(Exception):
    pass


def use_database_file(database) -> None:
    envDatabase = os.getenv('PATSY_DATABASE')
    if database is not None:
        database_helper(database)
    elif envDatabase is not None:
        database_helper(envDatabase)
    else:
        raise DatabaseNotSetError


def database_helper(database: str) -> None:
    # Set up database file or use in-memory db
    if database.startswith('postgresql+psycopg2:'):
        db_path = database
    else:
        db_path = f"sqlite:///{database}"

    str = ("Using a transient in-memory database..."
           if database == (':memory:')
           else f"Using database at {database}...")

    sys.stderr.write(f"{str}\n")
    sys.stderr.write("Binding the database session...\n")
    engine = create_engine(db_path)

    # Enable foreign key constraints
    if db_path.startswith('sqlite:'):
        event.listen(engine, 'connect',
                     lambda dbapi_con, con_record:
                     dbapi_con.execute('pragma foreign_keys=ON'))

    Session.configure(bind=engine)
