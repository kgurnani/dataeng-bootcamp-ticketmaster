from prefect import task


@task(log_prints=True, retries=3, retry_delay_seconds=5)
def get_event_data_as_dataframe(country, state):
    return 1


@task(log_prints=True)
def save_event_dataframe_to_gcs(df, country, state):
    return 1


@task(log_prints=True)
def refresh_event_data_in_bigquery(country, state):
    return 1


@task(log_prints=True)
def run_dbt_transformations():
    return 1
