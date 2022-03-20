from datetime import timedelta
from prefect import Flow
from prefect.schedules import IntervalSchedule
from tasks import (
    create_raw_table,
    create_parsed_table,
    retrieve_parsed_states,
    get_raw_states_from_api,
    store_raw_states,
    parse_raw_states,
    store_parsed_states,
    filter_new_states,
    transform_states_to_count_origin_countries,
    DB_FILE
)

schedule_etl = IntervalSchedule(interval=timedelta(minutes=1))
with Flow('API_TRANSFORM_STORE_ETL', schedule=schedule_etl) as rtl_flow:
    raw_table = create_raw_table()
    parsed_table = create_parsed_table()

    raw_data = get_raw_states_from_api()
    populate_raw_table = store_raw_states(raw_data, db=DB_FILE)
    populate_raw_table.set_upstream(raw_table)

    parsed_data = parse_raw_states(raw_data)
    filtered_data = filter_new_states(
            parsed_data, retrieve_parsed_states())

    populate_parsed_table = store_parsed_states(filtered_data, db=DB_FILE)
    populate_parsed_table.set_upstream(parsed_table)

rtl_flow.visualize()

schedule_elt = IntervalSchedule(interval=timedelta(minutes=3))
with Flow('DB_TRANSFORM_DB_ELT', schedule=schedule_elt) as elt_flow:
    # parsed_states = tasks.
    data = retrieve_parsed_states()
    counts = transform_states_to_count_origin_countries(data)
    print(f'***----*** Spanish planes: {counts["Spain"]} ***----***')
elt_flow.visualize()
