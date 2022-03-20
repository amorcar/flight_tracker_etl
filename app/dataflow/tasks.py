import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import Tuple, Counter, List
from prefect import task
from prefect.tasks.database.sqlite import SQLiteScript, SQLiteQuery

from opensky import (
    FlightState,
    get_flights_in_roi,
    parse_flight_states,
)
from database import (
    insert_raw_states_in_db,
    insert_parsed_states_in_db,
    insert_countries_in_db,
)
from operations import (
    filter_duplicated_states,
    count_countries_of_origin,
    save_plot_countries_histogram,
)

DB_FILE = 'test.sqlite'

create_raw_table = SQLiteScript(
    db=DB_FILE,
    script = 'CREATE TABLE IF NOT EXISTS raw_state (\
        icao24 TEXT,\
        callsign TEXT,\
        origin_country TEXT,\
        time_position INTEGER,\
        last_contact INTEGER,\
        longitude FLOAT,\
        latitude FLOAT,\
        baro_altitude FLOAT,\
        on_ground INTEGER,\
        velocity FLOAT,\
        true_track FLOAT,\
        vertical_rate FLOAT,\
        geo_altitude FLOAT,\
        squawk FLOAT,\
        spi INTEGER,\
        position_source INTEGER\
        )'
)
create_parsed_table = SQLiteScript(
    db=DB_FILE,
    script = 'CREATE TABLE IF NOT EXISTS state (\
        icao24 TEXT,\
        callsign TEXT,\
        origin_country TEXT,\
        on_ground INTEGER,\
        spi INTEGER,\
        last_contact INTEGER\
        )'
)

create_countries_table = SQLiteScript(
    db=DB_FILE,
    script = 'CREATE TABLE IF NOT EXISTS countries (\
        country TEXT,\
        count INTEGER,\
        UNIQUE (country)\
        )'
)

retrieve_raw_states = SQLiteQuery(
    db=DB_FILE,
    query='SELECT * FROM raw_state'
)

retrieve_parsed_states = SQLiteQuery(
    db=DB_FILE,
    query='SELECT * FROM state'
)

retrieve_countries_count = SQLiteQuery(
    db=DB_FILE,
    query='SELECT * FROM countries'
)

@task(name='get-api-data')
def get_raw_states_from_api() -> Tuple:
    # Manhattan ROI
    lamax, lomax = 41.20628875958395, -72.98751255641236
    lamin, lomin = 40.47989847518386, -74.584242519867

    api_data = get_flights_in_roi(lamax, lomax, lamin, lomin)
    return  api_data


@task
def store_raw_states(raw_states: List, db: str):
    insert_raw_states_in_db(db, raw_states)

@task
def store_parsed_states(raw_states: List, db: str):
    insert_parsed_states_in_db(db, raw_states)

@task
def store_countries_count(countries_count: dict, db: str):
    insert_countries_in_db(db, countries_count)

@task
def parse_raw_states(raw_states: List) -> Tuple[FlightState]:
    parsed_data = parse_flight_states(raw_states)
    return parsed_data

@task
def transform_state_tuples_to_dataclass(states):
    flight_states = [FlightState(*state) for state in states]
    def convert_to_bools(state):
        values = list(state)
        #TODO: This is reallly awful, improve
        bool_indices = (3, 4)
        for i in bool_indices:
            values[i] = bool(values[i])
        return FlightState(*values)

    return [convert_to_bools(state) for state in flight_states]

@task
def filter_new_states(
        new_states: List[FlightState],
        stored_states: List[FlightState]
        ) -> Tuple[FlightState]:
    filtered_parsed_states = filter_duplicated_states(new_states, stored_states)
    return filtered_parsed_states

@task
def transform_states_to_count_origin_countries(states: List[FlightState]) -> Counter:
    counts = count_countries_of_origin(states)
    return counts

@task
def transform_count_tuples_to_dict(count_tuple):
    count_dict = {c: v for (c, v) in count_tuple}
    return count_dict

@task
def save_histogram_task(count: dict):
    save_plot_countries_histogram(count)
