from prefect import flow
from event_data_refresh_tasks import (
    get_events_data_as_dataframe,
    save_events_dataframe_to_gcs,
    refresh_events_data_in_bigquery,
)


@flow(name="Get upcoming events data", log_prints=True)
def get_upcoming_events_data(country: str, state: str):
    df = get_events_data_as_dataframe(country, state)
    print(df.head(n=10).to_string())
    gcs_file_name = save_events_dataframe_to_gcs(df, country, state)
    # refresh_events_data_in_bigquery(country, state, df)


if __name__ == "__main__":
    get_upcoming_events_data("CA", "ON")
