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

class JourneySummary(Base):
    __tablename__ = "journey_summary"
    id = Column(Integer, primary_key=True)
    line_ref = Column(String(10))
    direction_ref = Column(String(20))
    origin_ref = Column(String(20))
    origin_name = Column(String(100))
    destination_ref = Column(String(20))
    destination_name = Column(String(100))
    vehicle_ref = Column(String(25))
    vehicle_journey_date_ref = Column(String(40))
    journey_date_line_ref = Column(String(40), unique=True)
    hour = Column(DateTime)
    num_points_stationary = Column(Integer)
    num_points = Column(Integer)
    time_intv_med = Column(Float)
    time_total_hrs = Column(Float)
    time_intv_mean = Column(Float)
    time_intv_min = Column(Float)
    time_intv_max = Column(Float)
    dist_total_miles = Column(Float)
    dist_intv_med_miles = Column(Float)
    dist_intv_mean_miles = Column(Float)
    speed_min_mph = Column(Float)
    speed_max_mph = Column(Float)
    speed_med_mph = Column(Float)
    speed_mean_mph = Column(Float)


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
