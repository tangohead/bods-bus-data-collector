import time
import argparse
import logging
import json
import xml.etree.ElementTree
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone

import dateutil.parser
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
import pandas as pd
import boto3

from bus_data_models import Base, BusLocation
import credentials

BODS_LOCATION_API_URL = (
    "https://data.bus-data.dft.gov.uk/api/v1/datafeed?operatorRef={}&api_key={}"
)


def convert_activity_to_dict(activity: xml.etree.ElementTree.Element) -> dict:
    """
    Helper function to unpack activities into JSON.

    Parameters
    ----------
    activity : xml.etree.ElementTree.Element
        An XML element describing the location and associated information of a bus.

    Returns
    -------
    dict
        Dictionary descriving the location and associated information of a bus.

    """

    vehicle_journey = activity.find(
        "{http://www.siri.org.uk/siri}MonitoredVehicleJourney"
    )
    vehicle_location = vehicle_journey.find(
        "{http://www.siri.org.uk/siri}VehicleLocation"
    )
    return {
        "entry_id": activity.find("{http://www.siri.org.uk/siri}ItemIdentifier").text,
        "timestamp": activity.find("{http://www.siri.org.uk/siri}RecordedAtTime").text,
        "line_ref": vehicle_journey.find("{http://www.siri.org.uk/siri}LineRef").text,
        "direction_ref": vehicle_journey.find(
            "{http://www.siri.org.uk/siri}DirectionRef"
        ).text,
        "line_name": vehicle_journey.find(
            "{http://www.siri.org.uk/siri}PublishedLineName"
        ).text,
        "operator_ref": vehicle_journey.find(
            "{http://www.siri.org.uk/siri}OperatorRef"
        ).text,
        "origin_ref": vehicle_journey.find(
            "{http://www.siri.org.uk/siri}OriginRef"
        ).text,
        "origin_name": vehicle_journey.find(
            "{http://www.siri.org.uk/siri}OriginName"
        ).text,
        "destination_ref": vehicle_journey.find(
            "{http://www.siri.org.uk/siri}DestinationRef"
        ).text,
        "destination_name": vehicle_journey.find(
            "{http://www.siri.org.uk/siri}DestinationName"
        ).text,
        "origin_aimed_departure_time": vehicle_journey.find(
            "{http://www.siri.org.uk/siri}OriginAimedDepartureTime"
        ).text,
        "vehicle_lat": float(
            vehicle_location.find("{http://www.siri.org.uk/siri}Latitude").text
        ),
        "vehicle_lon": float(
            vehicle_location.find("{http://www.siri.org.uk/siri}Longitude").text
        ),
        "vehicle_bearing": float(
            vehicle_journey.find("{http://www.siri.org.uk/siri}Bearing").text
        ),
        "vehicle_journey_ref": vehicle_journey.find(
            "{http://www.siri.org.uk/siri}VehicleJourneyRef"
        ).text,
        "vehicle_ref": vehicle_journey.find(
            "{http://www.siri.org.uk/siri}VehicleRef"
        ).text,
    }


def output_json(bus_loc_list: list, output_path: Path):
    """
    Takes a list of bus location dictionaries, prepares them for export then
    dumps them as JSON.

    Parameters
    ---------
    bus_loc_list: list
        A list of bus location report dictionaries.
    output_path: Path
        Path to save JSON to.

    Returns
    -------
    json_str : str
        A string representation of the JSON object.
    """

    # Convert to a DF and get rid of some of the columns not useful on the
    # front end
    output_df = pd.DataFrame(json_output_list).drop(
        ["entry_id", "origin_ref", "destination_ref", "line_ref"], axis=1
    )
    # Remove some unnecessary charaters
    output_df.loc[output_df["direction_ref"] == "INBOUND", ["direction_ref"]] = "I"
    output_df.loc[output_df["direction_ref"] == "OUTBOUND", ["direction_ref"]] = "O"

    # We want to write and have the option to put it on S3, so we do it
    # this way
    json_str = output_df.to_json(None, orient="records")
    with open(output_path, "w") as f:
        f.write(json_str)

    return json_str


def add_bus_location_to_db_session(bus_loc_report: dict, db_session: Session):
    """
    Simply adds a bus location report to the current database session. Note that this function converts string ISO timestamps to database objects.

    Parameters
    ----------
    bus_loc_report : dict
        A bus location report, as prepared by convert_activity_to_dict.
    db_session : Session
        An SQLAlchemy database session.

    Returns
    -------
    Nothing.

    """
    bus_location = BusLocation(
        entry_id=bus_loc_report["entry_id"],
        timestamp=dateutil.parser.isoparse(bus_loc_report["timestamp"]),
        line_ref=bus_loc_report["line_ref"],
        direction_ref=bus_loc_report["direction_ref"],
        line_name=bus_loc_report["line_name"],
        operator_ref=bus_loc_report["operator_ref"],
        origin_ref=bus_loc_report["origin_ref"],
        origin_name=bus_loc_report["origin_name"],
        destination_ref=bus_loc_report["destination_ref"],
        destination_name=bus_loc_report["destination_name"],
        origin_aimed_departure_time=dateutil.parser.isoparse(
            bus_loc_report["origin_aimed_departure_time"]
        ),
        vehicle_lat=bus_loc_report["vehicle_lat"],
        vehicle_lon=bus_loc_report["vehicle_lon"],
        vehicle_bearing=bus_loc_report["vehicle_bearing"],
        vehicle_journey_ref=bus_loc_report["vehicle_journey_ref"],
        vehicle_ref=bus_loc_report["vehicle_ref"],
    )
    session.add(bus_location)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Tool to collect and publish the latest BODS data for a given operator.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--db",
        help="Save each update to a database.",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "operator_code", help="The BODS operator code to grab.", type=str
    )
    parser.add_argument(
        "output_path", help="Location to save each update to.", type=str
    )
    parser.add_argument(
        "--aws",
        help="Push to S3 Bucket on each update.",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--aws_filename",
        help="Name to push to S3 bucket.",
        type=str,
        default="current_bus_locations.json",
    )
    parser.add_argument(
        "--sleep_interval",
        help="How many seconds to sleep between each pull from the API.",
        type=int,
        default=6,
    )
    parser.add_argument(
        "--aws_push_interval",
        help="The number of sleep cycles to wait between pushing data to AWS.",
        type=int,
        default=3
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler("data_collector.log"),
            logging.StreamHandler()
        ]
    )

    # Check output path validity
    output_path = Path(args.output_path)
    if output_path.is_dir():
        raise ValueError("Output path cannot be a directory.")
    if output_path.exists():
        print("Path {} exists - will be overwritten.".format(output_path))

    # Set up the DB
    if args.db:
        engine = create_engine(
            "postgresql://{}:{}@{}:{}".format(
                credentials.POSTGRES_USER,
                credentials.POSTGRES_PASSWORD,
                credentials.POSTGRES_HOST,
                credentials.POSTGRES_PORT,
            )
        )
        Base.metadata.bind = engine

        DBSession = sessionmaker(bind=engine)
        session = DBSession()

    # Set up AWS
    if args.aws:
        s3 = boto3.resource("s3")

    # Set up operator code and URL to query
    operator_ref = args.operator_code
    location_url = BODS_LOCATION_API_URL.format(operator_ref, credentials.BODS_API_KEY)

    # Loop to get latest data
    aws_interval_counter = 0
    while True:
        try:
            # Get the latest info
            resp = requests.get(location_url)
            tree = ET.fromstring(resp.text)

            # Extract the activities
            activities = tree.findall(
                "./{http://www.siri.org.uk/siri}ServiceDelivery/{http://www.siri.org.uk/siri}VehicleMonitoringDelivery/{http://www.siri.org.uk/siri}VehicleActivity"
            )

            # Convert each to JSON
            json_output_list = []
            for activity in activities:
                converted_activity = convert_activity_to_dict(activity)
                json_output_list.append(converted_activity)

                # Add to database if needed
                if args.db:
                    add_bus_location_to_db_session(converted_activity, session)

            json_str = output_json(json_output_list, output_path)

            aws_interval_counter += 1

            # if using AWS, push to bucket
            if args.aws and aws_interval_counter >= args.aws_push_interval:
                aws_interval_counter = 0
                s3.Bucket(credentials.S3_BUCKET_NAME).put_object(
                    Key=args.aws_filename, Body=json_str, ACL="public-read"
                )
                

            # Commit to Database
            if args.db:
                session.commit()
        except Exception as e:
            logging.error("Error getting data: {}".format(e))

        time.sleep(args.sleep_interval)
