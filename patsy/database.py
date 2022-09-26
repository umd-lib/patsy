import argparse
import sys

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from .model import Base

Session = sessionmaker()#autocommit=True)

def use_database_file(database: str) -> None:
    # Set up database file or use in-memory db
    if database.startswith('postgresql+psycopg2:'):
        db_path = database
    else:
        db_path = f"sqlite:///{database}"

    str = ("Using a transient in-memory database..." 
            if database == (':memory:') 
            else f"Using database at {database}...")

    sys.stderr.write(str)
    sys.stderr.write("Binding the database session...")

    engine = create_engine(db_path)#, echo=True)

    # Enable foreign key constraints
    if db_path.startswith('sqlite:'):
        event.listen(engine, 'connect',
                        lambda dbapi_con, con_record:
                            dbapi_con.execute('pragma foreign_keys=ON')) 

    Session.configure(bind=engine)
