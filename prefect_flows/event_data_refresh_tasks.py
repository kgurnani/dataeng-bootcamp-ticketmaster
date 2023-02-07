from prefect import task
from event_helpers import (
    build_events_url,
    extract_events_row_as_dict,
    get_utc_timestamp_in_seconds,
)
from pandas import DataFrame
from datetime import date, timedelta
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import BigQueryWarehouse

import httpx
import os
import time


@task(log_prints=True, retries=3, retry_delay_seconds=5)
def get_events_data_as_dataframe(country: str, state: str) -> DataFrame:
    tomorrow_datetime = (date.today() + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    ) + "T00:00:00Z"
    events_url = build_events_url(country, state, tomorrow_datetime)

    print("Querying first page")
    response = httpx.get(events_url)
    response.raise_for_status()
    json_response = response.json()
    print("Queryied first page successfully")

    page = json_response["page"]
    total_pages = page["totalPages"]
    total_elements = page["totalElements"]
    print(f"Total pages: {total_pages}, Total elements: {total_elements}")

    max_page_to_query = min(5, total_pages)

    formatted_events = []
    events = json_response["_embedded"]["events"]

    for row in events:
        formatted_events.append(extract_events_row_as_dict(row))

    for i in range(1, max_page_to_query):
        time.sleep(3)
        print(f"Querying page {i + 1}")
        events_url = build_events_url(country, state, tomorrow_datetime, page_number=i)
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
def save_events_dataframe_to_gcs(df: DataFrame, country: str, state: str) -> str:
    timestamp = get_utc_timestamp_in_seconds()
    gcs_file_name = f"raw_data/events/{country}/{state}/{timestamp}.parquet.gzip"
    local_file_name = f"events_{country}_{state}_{timestamp}.parquet.gzip"
    df.to_parquet(f"/tmp/{local_file_name}", compression="gzip")

    gcs_block = GcsBucket.load("ticketmaster-bucket")
    gcs_block.upload_from_path(
        from_path=f"/tmp/{local_file_name}", to_path=gcs_file_name
    )

    os.remove(f"/tmp/{local_file_name}")

    return gcs_file_name


@task(log_prints=True)
def refresh_events_data_in_bigquery(country: str, state: str, df: DataFrame):
    with BigQueryWarehouse.load("ticketmaster-events-bq") as warehouse:
        create_table_operation = f"""
            CREATE TABLE IF NOT EXISTS ticketmaster.events (
                event_name
                event_tid
                event_url
                public_sale_start_date_time
                presale
                event_start_gmt_datetime
                event_start_local_date
                event_start_local_time
                event_timezone
            )
        """
        delete_operation = f"""
            DELETE FROM ticketmaster.events WHERE country_code = {country} AND state_code = {state}
        """
    return 1


@task(log_prints=True)
def run_dbt_transformations():
    return 1
