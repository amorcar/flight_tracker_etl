from datetime import timedelta, datetime
from dataclasses import asdict
from typing import Counter, List
from opensky import FlightState


def filter_duplicated_states(
        new_states: List[FlightState],
        stored_states: List[FlightState]
) -> List[FlightState]:

    current_stored_icao24_codes = [state.icao24 for state in stored_states]
    def icao24_code_already_exist_condition(state: FlightState) -> bool:
        return state.icao24 in current_stored_icao24_codes

    def last_update_more_than_one_day_ago_condition(
            state: FlightState,
            target_icao24: str
    ) -> bool:
        delta = timedelta(days=1)
        stored_timestamp = [
            *filter(lambda s: s.icao24 == target_icao24, stored_states)
        ][0].last_contact
        stored_datetime = datetime.fromtimestamp(stored_timestamp)
        new_datetime = datetime.fromtimestamp(state.last_contact)
        return new_datetime > stored_datetime + delta

    def apply_filter(state):
        target_icao24 = state.icao24
        first_filter = icao24_code_already_exist_condition(state)
        if not first_filter: return False
        second_filter = last_update_more_than_one_day_ago_condition(state, target_icao24)
        return first_filter and second_filter

    filtered_states = filter(apply_filter, new_states)

    return [*filtered_states]

def count_countries_of_origin(states: List[FlightState]) -> Counter:
    # 1 convert FlightState dataclass to dict
    dict_states = [*map(asdict, states)]
    # 2 count the different countries of origin
    counter = Counter([ state['origin_country'] for state in dict_states ])
    return counter


if __name__ == '__main__':
    import copy
    from opensky import *

    # Manhattan ROI
    lamax, lomax = 41.20628875958395, -72.98751255641236
    lamin, lomin = 40.47989847518386, -74.584242519867

    print('Calling the API...')
    flights = get_flights_in_roi(lamax, lomax, lamin, lomin)

    if not flights:
        print('No API results')
        exit()
    print('parsing states...')
    parsed_states = parse_flight_states(flights)
    new_parsed = copy.deepcopy(parsed_states)

    filtered_empty = filter_duplicated_states(
            new_states=new_parsed, stored_states=parsed_states)
    assert not filtered_empty

    new_state = copy.deepcopy(new_parsed[0])
    new_state_timestamp = datetime.fromtimestamp(new_state.last_contact)
    two_days_delta = timedelta(days=2)
    new_state.last_contact = int((new_state_timestamp + two_days_delta).timestamp())
    new_parsed.append(new_state)

    filtered = filter_duplicated_states(new_parsed, parsed_states)
    print('Everything OK')
    assert len(filtered), 1

    from pprint import pprint as pp
    pp(count_countries_of_origin(filtered))

