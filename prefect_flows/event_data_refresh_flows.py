from prefect import flow
from event_data_refresh_tasks import get_events_data_as_dataframe


@flow(name="Get upcoming events data", log_prints=True)
def get_upcoming_events_data(country, state):
    df = get_events_data_as_dataframe(country, state)
    print(df.head(n=10).to_string())


if __name__ == "__main__":
    get_upcoming_events_data("CA", "AB")
