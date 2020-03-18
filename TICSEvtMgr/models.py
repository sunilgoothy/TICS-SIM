from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table

#Create and engine and get the metadata
Base = declarative_base()
engine = create_engine('sqlite:///database.db')
metadata = MetaData(bind=engine)
# create a configured "Session" class

Session = sessionmaker(bind=engine)

# create a Session
session = Session()

#Reflect each database table we need to use, using metadata
class PDI(Base):
    __table__ = Table('r_PDI', metadata, autoload=True)
    def __repr__(self):
        return '<PDI %r>' % self.cid   

class PDO(Base):
    __table__ = Table('r_PDO', metadata, autoload=True)
    def __repr__(self):
        return '<PDI %r>' % self.cid 