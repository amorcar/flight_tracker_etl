from datetime import timedelta
from prefect import Flow, Parameter
from prefect.schedules import IntervalSchedule
from dataflow.tasks import (
    create_raw_table,
    create_parsed_table,
    create_countries_table,
    retrieve_parsed_states,
    get_raw_states_from_api,
    store_raw_states,
    parse_raw_states,
    store_parsed_states,
    filter_new_states,
    transform_states_to_count_origin_countries,
    store_countries_count,
    retrieve_countries_count,
    save_histogram_task,
    transform_count_tuples_to_dict,
    transform_state_tuples_to_dataclass,
    DB_FILE
)


schedule_etl = IntervalSchedule(interval=timedelta(minutes=15))
with Flow('API_TRANSFORM_STORE_ETL', schedule=schedule_etl) as etl_flow:
    raw_table = create_raw_table()
    parsed_table = create_parsed_table()

    # Get data from API
    raw_data = get_raw_states_from_api()
    # Store raw data in DB
    populate_raw_table = store_raw_states(raw_data, db=DB_FILE)
    populate_raw_table.set_upstream(raw_table)

    # Parse raw data to dataclass
    parsed_data = parse_raw_states(raw_data)
    # Retrieve already stored parsed data
    stored_parsed_states = retrieve_parsed_states()
    stored_parsed_states.set_upstream(parsed_table)
    # Transform retrieved data to dataclass
    stored_dc = transform_state_tuples_to_dataclass(stored_parsed_states)
    # Filter new data to avoid duplicates
    filtered_data = filter_new_states(parsed_data, stored_dc)

    # Store the filtered data in DB
    populate_parsed_table = store_parsed_states(filtered_data, db=DB_FILE)
    populate_parsed_table.set_upstream(parsed_table)


    #TODO: Separate this into a different flow
    countries_table = create_countries_table()
    # Retrieve parsed data (again) from DB
    data = retrieve_parsed_states()
    data.set_upstream(parsed_table)
    # Transform retrieved data to dataclass
    parsed_data = transform_state_tuples_to_dataclass(data)
    # Transform data to count of countries
    counts = transform_states_to_count_origin_countries(parsed_data)
    # Store the count of origin countries
    stored_counts = store_countries_count(counts, db=DB_FILE)


    #TODO: Separate this into a different flow
    # Retrieve countries count from DB
    retrieved_counts = retrieve_countries_count()
    retrieved_counts.set_upstream(stored_counts)
    retrieved_counts.set_upstream(countries_table)
    # Transform retrieved tuples to Count(dict) type
    count_dict = transform_count_tuples_to_dict(retrieved_counts)
    # Create histogram from data dict
    saved_plot = save_histogram_task(count_dict)




# etl_flow.visualize()
# etl_flow.run()

# schedule_elt = IntervalSchedule(interval=timedelta(minutes=3))
# with Flow('DB_TRANSFORM_DB_ELT', schedule=schedule_elt) as elt_flow:
#     parsed_table = Parameter('parsed_table')
#     countries_table = create_countries_table()
#     data = retrieve_parsed_states()
#     data.set_upstream(parsed_table)
#     counts = transform_states_to_count_origin_countries(data)
#     stored_counts = store_countries_count(counts, db=DB_FILE)
# elt_flow.visualize()

# from prefect.tasks.prefect.flow_run import create_flow_run
# from prefect.tasks.prefect import F

# with Flow('PARENT_FLOW') as p_flow:
#     ...
#     flow_etl = FlowRunTask(flow_name='API_TRANSFORM_STORE_ETL')
#     flow_etl = FlowRunTask(flow_name='DB_TRANSFORM_DB_ELT')
# p_flow.visualize()
# p_flow.run()


