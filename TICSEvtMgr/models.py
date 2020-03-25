from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy import create_engine, MetaData, Table

#Create and engine and get the metadata
Base = declarative_base()
#engine = create_engine('sqlite:///database.db')
engine = create_engine('sqlite:///database.db',connect_args={'check_same_thread': False},poolclass=StaticPool, echo=False)
metadata = MetaData(bind=engine)
# create a configured "Session" class

Session = sessionmaker(bind=engine)

# create a Session
session = Session()

#Reflect each database table we need to use, using metadata
class PDI(Base):
    __table__ = Table('r_PDI', metadata, autoload=True)
    def __repr__(self):
        return '<PDI %r>' % self.c_SlabID   

class PDO(Base):
    __table__ = Table('r_PDO', metadata, autoload=True)
    def __repr__(self):
        return '<PDO %r>' % self.c_SlabID

class r_Shift_Record(Base):
    __table__ = Table('r_Shift_Record', metadata, autoload=True)
    def __repr__(self):
        return '<r_Shift_Record %r>' % self.i_ShiftIndex

class r_Delay_Record(Base):
    __table__ = Table('r_Delay_Record', metadata, autoload=True)
    def __repr__(self):
        return '<r_Delay_Record %r>' % self.i_DelayIndex 