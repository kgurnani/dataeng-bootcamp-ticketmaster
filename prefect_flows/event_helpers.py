from prefect.blocks.system import String
from calendar import timegm
from datetime import datetime


def build_events_url(
    country_code: str,
    state_code: str,
    start_datetime: str,
    page_number: int = 0,
) -> str:
    """Builds query string for the events route"""
    api_key_block = String.load("ticketmaster-api-key")
    api_key = api_key_block.value
    return f"https://app.ticketmaster.com/discovery/v2/events?apikey={api_key}&locale=en-us&startDateTime={start_datetime}&size=200&page={page_number}&sort=date,asc&countryCode={country_code}&stateCode={state_code}"


def extract_events_row_as_dict(row: dict) -> dict:
    return {
        "event_name": row["name"],
        "event_tid": row["id"],
        "public_sale_start_date_time": (
            row["sales"]["public"].get(
                "startDateTime", row["dates"]["start"]["localDate"] + "T00:00:00Z"
            )
        )
        .replace("T", " ")
        .replace("Z", ""),
        "presale": True if "presales" in row["sales"] else False,
        "event_start_utc_date_time": (
            row["dates"]["start"].get(
                "dateTime", row["dates"]["start"]["localDate"] + "T00:00:00Z"
            )
        )
        .replace("T", " ")
        .replace("Z", ""),
        "event_start_local_date": row["dates"]["start"]["localDate"],
        "event_start_local_time": row["dates"]["start"].get("localTime", "00:00:00"),
        "event_timezone": row["dates"].get("timezone", "NONE"),
        "segment": row["classifications"][0].get("segment", {}).get("name", "NONE"),
        "genre": row["classifications"][0].get("genre", {}).get("name", "NONE"),
        "sub_genre": row["classifications"][0].get("subGenre", {}).get("name", "NONE"),
        "promoter": row.get("promoter", {}).get("name", "NOT SPECIFIED"),
        "venue_name": row["_embedded"]["venues"][0]["name"],
        "venue_tid": row["_embedded"]["venues"][0]["id"],
        "venue_timezone": row["_embedded"]["venues"][0]["timezone"],
        "venue_city_name": row["_embedded"]["venues"][0]["city"]["name"],
        "venue_state_name": row["_embedded"]["venues"][0]["state"]["name"],
        "venue_state_code": row["_embedded"]["venues"][0]["state"]["stateCode"],
        "venue_country_name": row["_embedded"]["venues"][0]["country"]["name"],
        "venue_country_code": row["_embedded"]["venues"][0]["country"]["countryCode"],
        "number_of_attractions": len(row["_embedded"].get("attractions", [])),
    }


def get_utc_timestamp_in_seconds() -> int:
    return timegm(datetime.utcnow().timetuple())
