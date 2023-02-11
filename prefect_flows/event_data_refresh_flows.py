from prefect import flow
from event_data_refresh_tasks import (
    get_events_data_as_dataframe,
    save_events_dataframe_to_gcs,
    prepare_bigquery_for_insert,
)
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_insert_stream
from prefect.blocks.system import String
from prefect_dbt.cli.commands import trigger_dbt_cli_command


@flow(name="Get upcoming events data", log_prints=True)
def get_upcoming_events_data(country: str, state: str):
    df = get_events_data_as_dataframe(country, state)
    print(df.head(n=10).to_string())
    save_events_dataframe_to_gcs(df, country, state)
    prepare_bigquery_for_insert(country, state)

    gcp_project_string_block = String.load("de-zoomcamp-gcp-project")
    gcp_project = gcp_project_string_block.value

    gcp_credentials = GcpCredentials(project=gcp_project)
    records = df.to_dict("records")
    result = bigquery_insert_stream(
        dataset="ticketmaster",
        table="events",
        records=records,
        gcp_credentials=gcp_credentials,
    )
    print(result)
    dbt_project_dir_block = String.load("de-capstone-dbt-project-dir")
    trigger_dbt_cli_command("dbt run", project_dir=dbt_project_dir_block.value)


if __name__ == "__main__":
    get_upcoming_events_data("CA", "ON")
