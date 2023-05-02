import logging
import os

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker


Session = sessionmaker()


class DatabaseNotSetError(Exception):
    pass


def use_database_file(database: str) -> None:
    """
    Configures the SQLAlchemy Session object, using either the provided string
    (assumed to be from the "database" command-line parameter), or the
    "PATSY_DATABASE" environment variable.

    Throws DatabaseNotSetError is the database connection URL is not defined
    by either of those methods.
    """
    db_connection_url = get_database_connection_url(database)
    database_helper(db_connection_url)


def get_database_connection_url(database_arg: str) -> str:
    """
    Parses and returns the database connection URL to provide to SQLAlchemy,
    using either the provided "database_arg" parameter, or the


    If both are defined, provided "database_arg" parameter takes precedence.

    Throws DatabaseNotSetError is the database connection URL is not defined
    by either of those methods.
    """
    database = None
    db_path = None
    envDatabase = os.getenv('PATSY_DATABASE')
    if database_arg is not None:
        database = database_arg
    elif envDatabase is not None:
        database = envDatabase
    else:
        raise DatabaseNotSetError

    if database.startswith('postgresql+psycopg2:'):
        db_path = database
        url = db_path.split('@', 1)[1]
        logging.info(f"Database: {url}")
    else:
        logging.debug("Switching to using SQLite as the adapter")
        db_path = f"sqlite:///{database}"
        logging.info(f"Database: {db_path}")

    return db_path


def database_helper(db_connection_url: str) -> None:
    """
    Helper method to actually configures the SQLAlchemy Session.

    Typically not called directly.
    """
    logging.debug("Binding the database session...")
    engine = create_engine(db_connection_url)

    # Enable foreign key constraints
    if db_connection_url.startswith('sqlite:'):
        event.listen(engine, 'connect',
                     lambda dbapi_con, con_record:
                     dbapi_con.execute('pragma foreign_keys=ON'))

    Session.configure(bind=engine)
