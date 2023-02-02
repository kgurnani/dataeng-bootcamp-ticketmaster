from prefect import flow, task
from prefect.blocks.system import String
from pandas import DataFrame
from time import sleep
import httpx

api_key_string_block = String.load("ticketmaster-api-key")


def get_url(api_key, country_code, state_code, page_size, page_number):
    return f"https://app.ticketmaster.com/discovery/v2/venues?apikey={api_key}&locale=*&size={page_size}&page={page_number}&countryCode={country_code}&stateCode={state_code}"


def extract_venue_row_data(row):
    ticketmaster_upcoming_events = (
        row["upcomingEvents"]["ticketmaster"]
        if "ticketmaster" in row["upcomingEvents"]
        else 0
    )
    url = row["url"] if "url" in row else ""
    address_line1 = row["address"]["line1"] if "address" in row else ""

    return {
        "name": row["name"],
        "type": row["type"],
        "id": row["id"],
        "url": url,
        "postal_code": row["postalCode"],
        "timezone": row["timezone"],
        "city_name": row["city"]["name"],
        "state_name": row["state"]["name"],
        "state_code": row["state"]["stateCode"],
        "country_name": row["country"]["name"],
        "country_code": row["country"]["countryCode"],
        "total_upcoming_events": row["upcomingEvents"]["_total"],
        "ticketmaster_upcoming_events": ticketmaster_upcoming_events,
        "address_line_1": address_line1,
    }


@task(retries=3, retry_delay_seconds=10, log_prints=True)
def get_venues_as_dataframe(country_code, state_code):
    venues_url = get_url(
        api_key=api_key_string_block.value,
        country_code=country_code,
        state_code=state_code,
        page_size=200,
        page_number=0,
    )
    print(f"Venues url to query is: {venues_url}")

    response = httpx.get(venues_url, headers={"access-control-allow-origin": "*"})
    response.raise_for_status()
    response = response.json()

    page_data = response["page"]
    print(f"Page data: {page_data}")

    venues_data = response["_embedded"]["venues"]
    data = []

    for row in venues_data:
        data.append(extract_venue_row_data(row))

    for i in range(1, page_data["totalPages"]):
        print(f"Getting page index: {i}")
        venues_url = get_url(
            api_key=api_key_string_block.value,
            country_code=country_code,
            state_code=state_code,
            page_size=200,
            page_number=i,
        )
        print(f"Url: {venues_url}")
        response = httpx.get(venues_url, headers={"access-control-allow-origin": "*"})
        response.raise_for_status()
        response = response.json()

        venues_data = response["_embedded"]["venues"]

        for row in venues_data:
            data.append(extract_venue_row_data(row))

        sleep(5)

    venues_df = DataFrame(data)

    return venues_df


@task(log_prints=True)
def update_df_schema(df):
    df.astype({"total_upcoming_events": int, "ticketmaster_upcoming_events": int})
    return df


@flow(name="Update venues data", log_prints=True)
def update_venues(country_code="CA", state_code="AB"):
    venues_df = get_venues_as_dataframe(
        country_code=country_code, state_code=state_code
    )
    venues_df = update_df_schema(venues_df)
    print(f"Total df len: {len(venues_df)}")
    print(venues_df.head(10).to_string())
    print("Completed flow run.")


if __name__ == "__main__":
    update_venues()
