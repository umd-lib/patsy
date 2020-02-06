import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


class Database():
    """
    Class for managing connection to a sqlite database
    """

    def __init__(self, path):
        if path == ":memory:":
            print(f"Using a transient in-memory database...")
        else:
            print(f"Using database at {path}...")
        self.engine = create_engine(f'sqlite:///{path}', echo=True)
        print("Creating the schema using the declarative base...")
        Base = declarative_base()
        Base.metadata.create_all(self.engine)

    def session(self):
        Session = sessionmaker(bind=self.engine)
        return Session()
