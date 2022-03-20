import requests
from dataclasses import dataclass, fields
from typing import List, Optional


STATE_RESPONSE_ORDER = {
    'icao24': 0,
    'callsign': 1,
    'origin_country': 2,
    'time_position': 3,
    'last_contact': 4,
    'longitude': 5,
    'latitude': 6,
    'baro_altitude': 7,
    'on_ground': 8,
    'velocity': 9,
    'true_track': 10,
    'vertical_rate': 11,
    'sensors': 12,
    'geo_altitude': 13,
    'squawk': 14,
    'spi': 15,
    'position_source': 16
}


def get_flights_in_roi(
        max_lat: float,
        max_lon: float,
        min_lat: float,
        min_lon: float
) -> List:
    '''
    Retrieve live flight information about planes inside the given regoion of
    intereset using the OpenSky API.

    Raises error when the response status code is not 200
    '''
    api_root_url = 'https://opensky-network.org/api'
    roi_url = f'{api_root_url}/states/all?\
            lamin={min_lat}&\
            lomin={min_lon}&\
            lamax={max_lat}&\
            lomax={max_lon}'

    response = requests.get(roi_url)

    #TODO: handle error codes better
    response.raise_for_status()

    response_json = response.json()

    return response_json['states']


@dataclass
class FlightState:
    icao24: str
    callsign: Optional[str]
    origin_country: str
    on_ground: bool
    spi: bool
    last_contact: int

    @classmethod
    def from_state_list(cls, state):
        return FlightState(
            **{key: state[STATE_RESPONSE_ORDER[key]]
            for key in FlightState.__dataclass_fields__.keys()}
        )

    def __iter__(self):
        return iter([ getattr(self, field.name) for field in fields(self) ])



def parse_flight_states(states: List) -> List[FlightState]:
    '''
    Parse the flight state list to a list of FlightState objects.
    '''
    parsed_states = []
    for state in states:
        parsed_state = FlightState.from_state_list(state)
        parsed_states.append(parsed_state)
    return parsed_states


# For debugging
#TODO: change for proper unit testing
if __name__ == '__main__':
    # Manhattan ROI
    lamax, lomax = 41.20628875958395, -72.98751255641236
    lamin, lomin = 40.47989847518386, -74.584242519867
    print('Calling the API...')
    flights = get_flights_in_roi(lamax, lomax, lamin, lomin)
    if flights:
        print(f'{len(flights)} planes found')
        print(flights[0])
        print(len(flights[0]))
        parsed_states = parse_flight_states(flights)
        print(parsed_states[0])



