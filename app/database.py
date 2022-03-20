import sqlite3
import copy
from contextlib import closing
from typing import List

from opensky import STATE_RESPONSE_ORDER as response_index
from opensky import FlightState

# sqlite3.register_adapter(bool, int)
# sqlite3.register_converter("BOOLEAN", lambda v: bool(int(v)))


def create_raw_state_table(db):
    '''
    Create a Raw State Table in the SQLite DB if it does not exits already.
    '''
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

    with closing(sqlite3.connect(db)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(script)
            conn.commit()

def create_parsed_state_table(db):
    '''
    Create a Parsed State Table in the SQLite DB if it does not exits already.
    '''
    script = 'CREATE TABLE IF NOT EXISTS state (\
        icao24 TEXT,\
        callsign TEXT,\
        origin_country TEXT,\
        on_ground INTEGER,\
        spi INTEGER,\
        last_contact INTEGER\
        )'

    with closing(sqlite3.connect(db)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(script)
            conn.commit()

def insert_raw_states_in_db(db, _raw_states):
    script = 'INSERT INTO raw_state VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,\
            ?, ?, ?, ?, ?)'

    raw_states = copy.deepcopy(_raw_states)
    for raw_state in raw_states:
        # Dont store the sensors vector which is always NULL
        raw_state.pop(response_index['sensors'])
        if len(raw_state) == 17:
            # Remove last element which does not appear in the API docs
            raw_state.pop()
    with closing(sqlite3.connect(db)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executemany(script, raw_states)
            conn.commit()


def insert_parsed_states_in_db(db, parsed_states):
    script = 'INSERT INTO state VALUES (?, ?, ?, ?, ?, ?)'

    with closing(sqlite3.connect(db)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executemany(script, map(tuple, parsed_states))
            conn.commit()

def get_raw_states_from_db(db):
    script = 'SELECT * FROM raw_state'

    with closing(sqlite3.connect(db)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(script)
            raw_states = cursor.fetchall()

    return raw_states

def get_parsed_states_from_db(db):
    script = 'SELECT * FROM state'

    with closing(sqlite3.connect(db)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(script)
            states = cursor.fetchall()

    flight_states =  [FlightState(*state) for state in states]

    def convert_to_bools(state):
        values = list(state)
        #TODO: This is reallly awful, improve
        bool_indices = (3, 4)
        for i in bool_indices:
            values[i] = bool(values[i])
        return FlightState(*values)

    return [convert_to_bools(state) for state in flight_states]

def create_country_count_table(db: str):
    '''
    Create a table to store the count of origin countries.
    '''
    script = 'CREATE TABLE IF NOT EXISTS countries (\
        country TEXT,\
        count INTEGER,\
        UNIQUE (country)\
        )'

    with closing(sqlite3.connect(db)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(script)
            conn.commit()

def insert_countries_in_db(db: str, country_counts: dict):
    first_script = 'INSERT OR IGNORE INTO countries VALUES (?, ?)'
    second_script = 'UPDATE countries SET count = count + ? WHERE country=?;'
    country_data = [*country_counts.items()]

    def reverse(tuples):
        new_tup = tuples[::-1]
        return new_tup

    with closing(sqlite3.connect(db)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executemany(first_script, [(c[0],0) for c in country_data])
            cursor.executemany(second_script, [*map(reverse, country_data)])
            conn.commit()

def get_country_counts_from_db(db: str) -> dict:
    script = 'SELECT * FROM countries'

    with closing(sqlite3.connect(db)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(script)
            countries_count = cursor.fetchall()

    countries_count_dict = {c: v for (c, v) in countries_count}
    return countries_count_dict


#TODO: change this for real unit testing
if __name__ == '__main__':
    import os
    from opensky import *
    db = 'test.sqlite'

    # Manhattan ROI
    lamax, lomax = 41.20628875958395, -72.98751255641236
    lamin, lomin = 40.47989847518386, -74.584242519867
    create_raw_state_table(db=db)
    create_parsed_state_table(db=db)

    print('Calling the API...')
    flights = get_flights_in_roi(lamax, lomax, lamin, lomin)

    if not flights:
        exit()
    print(f'{len(flights)} planes found')
    print(flights[0])
    print('Storing in raw db')
    insert_raw_states_in_db(db,flights)
    print('parsing states...')
    parsed_states = parse_flight_states(flights)
    print(parsed_states[0])
    print('Storing in parsed db...')
    insert_parsed_states_in_db(db, parsed_states)

    print('Fetching from parsed db...')
    restored_raw_rows = get_raw_states_from_db(db)
    restored_parsed_rows = get_parsed_states_from_db(db)
    print('First row:')
    print(restored_raw_rows[0])
    print(restored_parsed_rows[0])

    create_country_count_table(db)
    country_counts = {'Spain':2, 'USA':1}
    insert_countries_in_db(db, country_counts)
    country_counts = {'Spain':1, 'USA':4}
    insert_countries_in_db(db, country_counts)
    new_counts = get_country_counts_from_db(db)
    assert new_counts['Spain'] == 3

    print('Removing DB file...')
    os.remove(db)

