import datetime
import argparse
import json
import gzip
from io import BytesIO

import dateutil.rrule
import pandas as pd
from sqlalchemy import create_engine, and_, text
from sqlalchemy.orm import sessionmaker, Session
from geopy import distance
import boto3

from bus_data_models import Base, BusLocation, JourneySummary
import credentials


def preprocess_locations(
    raw_locations_df: pd.DataFrame, drop_threshold: int = 2
) -> pd.DataFrame:
    """"""
    # Lots of duplicates on position/timestamp for some reason (could be that they don't transmit when not moving?)
    raw_locations_df.drop_duplicates(
        subset=[
            "timestamp",
            "line_ref",
            "direction_ref",
            "vehicle_lat",
            "vehicle_lon",
            "vehicle_bearing",
        ],
        inplace=True,
    )
    # This is the 'hour' ref, i.e. the hour in which the bus departed
    # We will use this to do aggregate journey stats
    raw_locations_df["hour"] = raw_locations_df["origin_aimed_departure_time"].dt.floor(
        "h"
    )
    # Create a reference which ties a journey ref to a date - this is so we can handle
    # multiple days of 'journey refs' at once, which seem to cycle
    raw_locations_df["vehicle_journey_date_ref"] = (
        raw_locations_df["origin_aimed_departure_time"].dt.strftime("%Y-%m-%d")
        + "_"
        + raw_locations_df["vehicle_journey_ref"]
    )
    raw_locations_df["journey_date_line_ref"] = (
        raw_locations_df["operator_ref"]
        + "_"
        + raw_locations_df["origin_aimed_departure_time"].dt.strftime("%Y-%m-%dT%H:%M%s")
        + "_"
        + raw_locations_df["line_ref"]
        + "_"
        + raw_locations_df["vehicle_journey_ref"]
    )

    jdl_count = raw_locations_df.groupby("journey_date_line_ref")["id"].count()
    journeys_to_drop = jdl_count[jdl_count < drop_threshold].index.tolist()
    rows_to_drop = raw_locations_df[
        raw_locations_df["journey_date_line_ref"].isin(journeys_to_drop)
    ]

    raw_locations_df = raw_locations_df.drop(rows_to_drop.index)

    # To avoid weird re-ordering affects due to insertion order, we sort  by
    # timestamp then reset the index
    raw_locations_df = raw_locations_df.sort_values(by=["timestamp", "id"]).reset_index(
        drop=True
    )

    return raw_locations_df


def calculate_deltas(route_positions: pd.DataFrame) -> pd.DataFrame:
    """
    Given a list of bus positions, calculates the time and speed
    between position reports.
    """
    speed_times = []

    for (idx_a, pos_a), (idx_b, pos_b) in zip(
        route_positions[:-1].iterrows(), route_positions[1:].iterrows()
    ):
        dist = distance.distance(
            (pos_a.vehicle_lat, pos_a.vehicle_lon),
            (pos_b.vehicle_lat, pos_b.vehicle_lon),
        ).miles
        # Turn seconds to hours
        time = (pos_b.timestamp - pos_a.timestamp).total_seconds() / 3600
        if time != 0:
            speed = dist / time
            speed_times.append(
                {
                    "time": time,
                    "speed": speed,
                    "dist": dist,
                }
            )
    return pd.DataFrame(speed_times)


def summarise_journey_stats(
    route_deltas: pd.DataFrame,
    stopped_threshold: float = 1.0,
):
    # count stops
    return pd.Series(
        {
            "num_points_stationary": route_deltas[
                route_deltas["speed"] < stopped_threshold
            ]["speed"].count(),
            "num_points": route_deltas.shape[0],
            "time_total_hrs": route_deltas["time"].sum(),
            "time_intv_med": route_deltas["time"].median(),
            "time_intv_mean": route_deltas["time"].mean(),
            "time_intv_min": route_deltas["time"].min(),
            "time_intv_max": route_deltas["time"].max(),
            "dist_total_miles": route_deltas["dist"].sum(),
            "dist_intv_med_miles": route_deltas["dist"].median(),
            "dist_intv_mean_miles": route_deltas["dist"].mean(),
            "speed_min_mph": route_deltas["speed"].min(),
            "speed_max_mph": route_deltas["speed"].max(),
            "speed_med_mph": route_deltas["speed"].median(),
            "speed_mean_mph": route_deltas["speed"].mean(),
        }
    )


def summarise_hour_stats(hour_df: pd.DataFrame):
    return pd.Series(
        {
            "num_journeys": hour_df.shape[0],
            "num_stationary_mean": hour_df["num_stationary"].mean(),
            "num_stationary_med": hour_df["num_stationary"].median(),
            "num_stationary_min": hour_df["num_stationary"].min(),
            "num_stationary_max": hour_df["num_stationary"].max(),
            "num_points_mean": hour_df["num_points"].mean(),
            "num_points_med": hour_df["num_points"].median(),
            "num_points_max": hour_df["num_points"].max(),
            "num_points_min": hour_df["num_points"].min(),
            "time_total_mean_hrs": hour_df["time_total_hrs"].mean(),
            "time_total_med_hrs": hour_df["time_total_hrs"].mean(),
            "time_total_min_hrs": hour_df["time_total_hrs"].max(),
            "time_total_max_hrs": hour_df["time_total_hrs"].min(),
            "time_intv_mean": hour_df["time_intv_mean"].mean(),
            "time_intv_med": hour_df["time_intv_med"].mean(),
            "time_intv_min": hour_df["time_intv_min"].min(),
            "time_intv_max": hour_df["time_intv_max"].max(),
            "dist_total_mean_miles": hour_df["dist_total_miles"].mean(),
            "dist_total_med_miles": hour_df["dist_total_miles"].median(),
            "dist_total_min_miles": hour_df["dist_total_miles"].min(),
            "dist_total_max_miles": hour_df["dist_total_miles"].max(),
            "dist_intv_med_miles": hour_df["dist_intv_med_miles"].mean(),
            "dist_intv_mean_miles": hour_df["dist_intv_mean_miles"].mean(),
            "speed_min_mph": hour_df["speed_min_mph"].min(),
            "speed_max_mph": hour_df["speed_max_mph"].max(),
            "speed_med_mph": hour_df["speed_med_mph"].mean(),
            "speed_mean_mph": hour_df["speed_mean_mph"].mean(),
        }
    )


def summarise_journey(journey_df: pd.DataFrame) -> pd.DataFrame:
    metadata = journey_df[
        [
            "line_ref",
            "direction_ref",
            "origin_ref",
            "origin_name",
            "destination_ref",
            "destination_name",
            "hour",
            "vehicle_journey_date_ref",
            "journey_date_line_ref",
            "vehicle_ref",
        ]
    ].iloc[0]
    route_deltas = calculate_deltas(journey_df)
    journey_stats = summarise_journey_stats(route_deltas)

    return metadata.append(journey_stats)


def summarise_all_journeys(locations_df: pd.DataFrame):
    # First summarise all journeys
    return locations_df.groupby(["journey_date_line_ref"]).apply(summarise_journey)


def summarise_hour(summary_journey_df: pd.DataFrame) -> pd.Series:
    metadata = summary_journey_df[
        [
            "line_ref",
            "direction_ref",
            "origin_ref",
            "origin_name",
            "destination_ref",
            "destination_name",
            "hour",
            "vehicle_journey_date_ref",
            "journey_date_line_ref",
            "vehicle_ref",
        ]
    ].iloc[0]

    hour_stats = summarise_hour_stats(summary_journey_df)

    return metadata.append(hour_stats)


def summarise_all_hours(summarised_journeys: pd.DataFrame) -> pd.DataFrame:
    return (
        summarised_journeys.groupby(["direction_ref", "hour"])
        .apply(summarise_hour)
        .reset_index(drop=True)
    )


def convert_locations_to_hour_summaries(locations_df: pd.DataFrame()) -> pd.DataFrame:
    processed_locations_df = preprocess_locations(locations_df)
    # We need at least two locations to summarise journeys
    if processed_locations.shape[0] > 1:
        summarised_journeys_df = summarise_all_journeys(processed_locations_df)
        summarised_journey_hours_df = summarise_all_hours(summarised_journeys_df)

        return summarised_journey_hours_df
    else:
        return None


def convert_locations_to_journey_summaries(
    locations_df: pd.DataFrame(),
) -> pd.DataFrame:

    processed_locations_df = preprocess_locations(locations_df)
    if processed_locations_df.shape[0] >= 1:
        return summarise_all_journeys(processed_locations_df)
    else:
        return None

# def check_duplicate_journey_ref_ids(db_session: Session, check_df: pd.DataFrame) -> pd.DataFrame:
#     # We need to check for rogue rows inserted where the same journey ref has been used twice by mistake

#     # This doesn't work - might be in a different hour. Will need to check rows. 
#     # Could query to see if journey_date_line_ref exists, and if it does, either drop the row from the frame or from the DB in advance of the inser

#     # We first need to check if there are any duplicates in the frame itself
#     duplicated_refs = check_df.duplicated(["journey_date_line_ref"], keep=False)].copy()

#     if duplicated_refs.shape[0] > 0:
#         print("Found duplicated refs")
#         for ref in duplicated_refs["journey_date_line_ref"].unique():
#             single_duplicated_ref = duplicated_refs[duplicated_refs["journey_date_line_ref"] == ref]

#             leftover_rows = single_duplicated_ref.drop(single_duplicated_ref["num_points"].idxmax())

#             check_df = check_df.drop(leftover_rows)
    
#     # Now we check if there is an entry in the DB already for this journey_ref
#     # if so we check which has the higher point count. If in the DB we drop
#     # the row from the DF, otherwise we drop the DB row.


def process_day(
    db_session: Session, start_dt: datetime, end_dt: datetime, chunk_size: int = 1
):
    """
    Processes a specific day of data and puts the journey summaries in the corresponding
    table.

    Note that we filter the date by origin_aimed_departure_time as this is the same for all reports of a given bus journey. That means we capture the full journey even if it starts before midnight and runs past into the next day. Avoids cutting it off partway

    Parameters
    ----------
    db_session : Session
        An SQLAlchemy database session
    start_dt : datetime
        Start time of the day to process
    end_dt : datetime
        End time of the day to process
    chunk_size : int (default 1)
        Number of hours to process each loop iteration

    """
    print(start_dt)
    print(end_dt)
    # To allow this to be done on lower memory machines, we'll now do journeys hour by hour.
    day_rrule = dateutil.rrule.rrule(
        freq=dateutil.rrule.HOURLY,
        interval=chunk_size,
        dtstart=start_dt,
        until=end_dt - datetime.timedelta(hours=chunk_size),
    )
    offset_day_rrule = dateutil.rrule.rrule(
        freq=dateutil.rrule.HOURLY,
        interval=chunk_size,
        dtstart=start_dt + datetime.timedelta(hours=chunk_size),
        until=end_dt,
    )
    for start_hour, end_hour in zip(day_rrule, offset_day_rrule):
        print("{} to {}".format(start_hour, end_hour))
        hour_bus_locs_qry = (
            db_session.query(BusLocation)
            .filter(
                and_(
                    #         BusLocation.line_name == '8',
                    BusLocation.origin_aimed_departure_time >= start_hour,
                    BusLocation.origin_aimed_departure_time < end_hour,
                )
            )
            .order_by(BusLocation.id.asc())
        )
        hour_bus_locs_df = pd.read_sql(
            hour_bus_locs_qry.statement, hour_bus_locs_qry.session.bind
        )
        if hour_bus_locs_df.shape[0] > 1:
            hour_summaries_df = convert_locations_to_journey_summaries(hour_bus_locs_df)

            if hour_summaries_df is not None:
                session.bulk_insert_mappings(
                    JourneySummary, hour_summaries_df.to_dict(orient="records")
                )
                session.commit()
            else:
                print(
                    "Not enough bus locations to summaries in time period {} to {}".format(
                        start_hour, end_hour
                    )
                )
        else:
            print(
                "No valid journeys in time period {} to {}".format(start_hour, end_hour)
            )


def process_all_in_db(db_session: Session):
    """
    This processes all bus journeys in the database up to the end of the previous full day
    and inserts them into the JourneySummary table.

    Parameters
    ----------
    db_session : Session
        An SQLAlchemy database session.

    """
    # Get first and last entries of departure times
    first_dt_q = (
        db_session.query(BusLocation)
        .order_by(BusLocation.origin_aimed_departure_time.asc())
        .first()
    )
    first_dt = first_dt_q.origin_aimed_departure_time
    last_dt_q = (
        db_session.query(BusLocation)
        .order_by(BusLocation.origin_aimed_departure_time.desc())
        .first()
    )
    last_dt = last_dt_q.origin_aimed_departure_time

    # We now set up the starting dates - note we need the actual start
    # then the daily 'end', i.e. the next day
    first_date = first_dt.date()
    second_date = first_dt.date() + datetime.timedelta(days=1)
    last_date = last_dt.date()

    # Set up the recurrence rules
    first_rrule = dateutil.rrule.rrule(
        freq=dateutil.rrule.DAILY,
        dtstart=first_date,
        until=last_date - datetime.timedelta(days=1),
    )
    second_rrule = dateutil.rrule.rrule(
        freq=dateutil.rrule.DAILY,
        dtstart=second_date,
        until=last_date,
    )

    for day_start, day_end in zip(first_rrule, second_rrule):
        print("{} to {}".format(day_start, day_end))
        process_day(db_session, day_start, day_end)
        # break


def generate_daily_summary(
    db_session: Session,
    start_dt: datetime,
    num_detailed_days: int = 7,
    num_summary_days: int = 30,
) -> dict:
    # per day, per route, per hour summaries from past num_detailed days
    # per route, per hour summaries from past num_summary days
    # per hour summaries from past num_detailed days
    # Later - per route summaries from past num_detailed days
    # per hour summaries from past num_summary_days across all routes

    detailed_sql = """SELECT 
    MIN(num_points_stationary) AS num_points_stationary_min,
    MAX(num_points_stationary) AS num_points_stationary_max,
    AVG(num_points_stationary) AS num_points_stationary_avg,
    MIN(time_total_hrs) AS time_total_hrs_min,
    MAX(time_total_hrs) AS time_total_hrs_max,
    AVG(time_total_hrs) AS time_total_hrs_avg,
    MIN(speed_mean_mph) AS speed_mean_mph_min,
    MAX(speed_mean_mph) AS speed_mean_mph_max,
    AVG(speed_mean_mph) AS speed_mean_mph_avg,
    MIN(speed_med_mph) AS speed_med_mph_min,
    MAX(speed_med_mph) AS speed_med_mph_max,
    AVG(speed_med_mph) AS speed_med_mph_avg,
    line_ref,
    hour,
    COUNT(*) AS num_journeys
    FROM journey_summary
    WHERE hour <= date '{}' and hour > date '{}'
    GROUP BY hour, line_ref;""".format(
        start_dt - datetime.timedelta(days=1),
        start_dt - datetime.timedelta(days=num_detailed_days - 1),
    )

    summary_sql = """SELECT 
    MIN(num_points_stationary) AS num_points_stationary_min,
    MAX(num_points_stationary) AS num_points_stationary_max,
    AVG(num_points_stationary) AS num_points_stationary_avg,
    MIN(time_total_hrs) AS time_total_hrs_min,
    MAX(time_total_hrs) AS time_total_hrs_max,
    AVG(time_total_hrs) AS time_total_hrs_avg,
    MIN(speed_mean_mph) AS speed_mean_mph_min,
    MAX(speed_mean_mph) AS speed_mean_mph_max,
    AVG(speed_mean_mph) AS speed_mean_mph_avg,
    MIN(speed_med_mph) AS speed_med_mph_min,
    MAX(speed_med_mph) AS speed_med_mph_max,
    AVG(speed_med_mph) AS speed_med_mph_avg,
    line_ref,
    COUNT(*) AS num_journeys,
    extract('hour' from hour) AS hour_num
    FROM journey_summary
    WHERE hour <= date '{}' and hour > date '{}'
    GROUP BY hour_num, line_ref;""".format(
        start_dt - datetime.timedelta(days=1),
        start_dt - datetime.timedelta(days=num_summary_days - 1),
    )

    detailed_df = pd.read_sql(text(detailed_sql), db_session.bind)
    summary_df = pd.read_sql(text(summary_sql), session.bind)

    json_obj = {
        "detailed": detailed_df.to_dict(orient="records"),
        "summary": summary_df.to_dict(orient="records"),
        "num_days": num_detailed_days,
        "start": start_dt.strftime("%Y-%m-%d"),
    }

    return json.dumps(json_obj)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Tool to summarise bus journeys and push daily statistics to an S3 bucket.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--process_all",
        action="store_true",
        help="Process all data in the database, up until the end of the previous day to today.",
    )
    parser.add_argument(
        "--process_yesterday",
        action="store_true",
        help="Process all of yesterday's data.",
    )
    parser.add_argument(
        "--aws",
        action="store_true",
        help="Produce a summary JSON file and push it to an S3 bucket.",
    )
    args = parser.parse_args()

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

    if args.process_all:
        process_all_in_db(session)

    if args.process_yesterday:
        today = datetime.date.today()
        start_dt = datetime.datetime(today.year, today.month, today.day - 1)
        end_dt = datetime.datetime(today.year, today.month, today.day)
        process_day(session, start_dt, end_dt)

    if args.s3:
        daily_summary_json = generate_daily_summary(
            session, datetime.datetime.now() - datetime.timedelta(days=1)
        )

        s3 = boto3.resource("s3")
        upload_obj = BytesIO()
        json_comp = gzip.GzipFile(None, "w", 9, upload_obj)
        json_comp.write(daily_summary_json.encode("utf-8"))
        json_comp.close()
        s3.Bucket(credentials.S3_BUCKET_NAME).put_object(
            Key="daily_summary.json",
            Body=upload_obj.getvalue(),
            ACL="public-read",
            ContentType="application/json",
            ContentEncoding="gzip",
        )
