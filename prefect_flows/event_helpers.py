from prefect.blocks.system import String


def build_events_url(
    country_code: str, state_code: str, page_size: int = 200, page_number: int = 0
) -> str:
    """Builds query string for the events route"""
    api_key_block = String.load("ticketmaster-api-key")
    api_key = api_key_block.value
    return f"https://app.ticketmaster.com/discovery/v2/events?apikey={api_key}&locale=en-us&size={page_size}&page={page_number}&countryCode={country_code}&stateCode={state_code}"


def extract_events_row_as_dict(row: dict) -> dict:
    return {
        "event_name": row["name"],
        "event_tid": row["id"],
        "event_url": row["url"],
        "public_sale_start_date_time": row["sales"]["public"].get(
            "startDateTime", row["dates"]["start"]["localDate"] + "T00:00:00Z"
        ),
        "presale": True if "presales" in row["sales"] else False,
        "event_start_local_date": row["dates"]["start"]["localDate"],
        "event_start_local_time": row["dates"]["start"].get("localTime", "00:00:00"),
        "event_timezone": row["dates"].get("timezone", ""),
    }


def get_utc_timestamp_in_seconds() -> int:
    return 1


def get_most_recent_gcs_object(country, state) -> str:
    return 1
