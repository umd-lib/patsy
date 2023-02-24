import logging
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
        url = db_path.split('@', 1)[1]
        logging.info(f"Database: {url}")
    else:
        logging.debug("Switching to using SQLite as the adapter")
        db_path = f"sqlite:///{database}"
        logging.info(f"Database: {db_path}")

    logging.debug("Binding the database session...")
    engine = create_engine(db_path)

    # Enable foreign key constraints
    if db_path.startswith('sqlite:'):
        event.listen(engine, 'connect',
                     lambda dbapi_con, con_record:
                     dbapi_con.execute('pragma foreign_keys=ON'))

    Session.configure(bind=engine)
