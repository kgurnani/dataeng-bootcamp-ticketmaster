from prefect import task
from event_helpers import build_events_url, extract_events_row_as_dict
from pandas import DataFrame

import httpx


@task(log_prints=True, retries=3, retry_delay_seconds=5)
def get_events_data_as_dataframe(country: str, state: str) -> DataFrame:
    events_url = build_events_url(country, state)

    print("Querying first page")
    response = httpx.get(events_url)
    response.raise_for_status()
    json_response = response.json()
    print("Queryied first page successfully")

    page = json_response["page"]
    total_pages = page["totalPages"]
    total_elements = page["totalElements"]
    print(f"Total pages: {total_pages}, Total elements: {total_elements}")

    formatted_events = []
    events = json_response["_embedded"]["events"]

    for row in events:
        formatted_events.append(extract_events_row_as_dict(row))

    for i in range(1, total_pages):
        print(f"Querying page {i + 1}")
        events_url = build_events_url(country, state, page_number=i)
        response = httpx.get(events_url)
        response.raise_for_status()
        json_response = response.json()
        print(f"Queryied page {i + 1} successfully")
        events = json_response["_embedded"]["events"]
        for row in events:
            formatted_events.append(extract_events_row_as_dict(row))

    print("Returning df")
    return DataFrame(formatted_events)


@task(log_prints=True)
def save_events_dataframe_to_gcs(df, country, state):
    return 1


@task(log_prints=True)
def refresh_events_data_in_bigquery(country, state):
    return 1


@task(log_prints=True)
def run_dbt_transformations():
    return 1
