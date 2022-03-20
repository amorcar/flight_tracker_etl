from datetime import timedelta
from prefect import Flow
from prefect.schedules import IntervalSchedule
import tasks

schedule_etl = IntervalSchedule(interval=timedelta(minutes=1))
with Flow('API_TRANSFORM_STORE_ETL', schedule=schedule_etl) as rtl_flow:
    raw_table = tasks.create_raw_table()
    parsed_table = tasks.create_parsed_table()

    raw_data = tasks.get_raw_states_from_api()
    populate_raw_table = tasks.store_raw_states(raw_data, db=tasks.DB_FILE)
    populate_raw_table.set_upstream(raw_table)

    parsed_data = tasks.parse_flight_states(raw_data)
    filtered_data = tasks.filter_duplicated_states(
            parsed_data, tasks.retrieve_parsed_states())

    populate_parsed_table = tasks.store_parsed_states(filtered_data, db=tasks.DB_FILE)
    populate_parsed_table.set_upstream(parsed_table)

rtl_flow.visualize()

schedule_elt = IntervalSchedule(interval=timedelta(minutes=3))
with Flow('DB_TRANSFORM_DB_ELT', schedule=schedule_elt) as elt_flow:
    # parsed_states = tasks.
    data = tasks.retrieve_parsed_states()
    counts = tasks.count_countries_of_origin(data)
    print(f'***----*** Spanish planes: {counts["Spain"]} ***----***')
elt_flow.visualize()
