from sqlalchemy import create_engine, Column, Integer, Float, String, Date, ForeignKey, text
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import csv
from datetime import datetime

Base = declarative_base()

class Station(Base):
    __tablename__ = 'station'
    station = Column(String, primary_key=True)
    latitude = Column(Float)
    longitude = Column(Float)
    elevation = Column(Float)
    name = Column(String)
    country = Column(String)
    state = Column(String)
    records = relationship('StationRecords', backref='station_info')

class StationRecords(Base):
    __tablename__ = 'station_records'
    id = Column(Integer, primary_key=True)
    station = Column(String, ForeignKey('station.station'))
    date = Column(Date)
    precip = Column(Float)
    tobs = Column(Float)

engine = create_engine('sqlite:///weather_data.db')
Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

with open('clean_stations.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        station = Station(station=row['station'],
                          latitude=float(row['latitude']),
                          longitude=float(row['longitude']),
                          elevation=float(row['elevation']),
                          name=row['name'],
                          country=row['country'],
                          state=row['state'])
        session.add(station)

with open('clean_measure.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        station_records = StationRecords(station=row['station'],
                                         date=datetime.strptime(row['date'], '%Y-%m-%d'),
                                         precip=float(row['precip']),
                                         tobs=float(row['tobs']))
        session.add(station_records)

session.commit()

result = session.execute(text('SELECT * FROM station LIMIT 5'))
for row in result:
    print(row)
