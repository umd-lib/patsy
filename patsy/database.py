import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class Database():

    def __init__(self, path):
        self.engine = create_engine(f'sqlite:///{path}', echo=True)
    
    def session(self):
        return sessionmaker(bind=self.engine)
    