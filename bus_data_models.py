from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine

import credentials

Base = declarative_base()


class BusLocation(Base):
    __tablename__ = "bus_location"
    id = Column(Integer, primary_key=True)
    entry_id = Column(String(50))
    timestamp = Column(DateTime)
    line_ref = Column(String(10))
    direction_ref = Column(String(20))
    line_name = Column(String(10))
    operator_ref = Column(String(20))
    origin_ref = Column(String(20))
    origin_name = Column(String(100))
    destination_ref = Column(String(20))
    destination_name = Column(String(100))
    origin_aimed_departure_time = Column(DateTime)
    vehicle_lat = Column(Float)
    vehicle_lon = Column(Float)
    vehicle_bearing = Column(Float)
    vehicle_journey_ref = Column(String(50))
    vehicle_ref = Column(String(25))


if __name__ == "__main__":
    engine = create_engine(
        "postgresql://{}:{}@{}:{}".format(
            credentials.POSTGRES_USER,
            credentials.POSTGRES_PASSWORD,
            credentials.POSTGRES_HOST,
            credentials.POSTGRES_PORT,
        )
    )
    Base.metadata.create_all(engine)
    print("Done!")
