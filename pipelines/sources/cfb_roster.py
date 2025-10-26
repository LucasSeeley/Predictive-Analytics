import dlt
import requests

@dlt.resource(
    name="cfb_roster_source",
    primary_key="id",
    write_disposition="merge"
)
def cfb_roster_resource(api_key: str, year: int):
    url = "https://api.collegefootballdata.com/roster"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"year": year}

    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()

    for roster in data:
        yield {
            "id": roster.get("id"),
            "first_name": roster.get("firstName"),
            "last_name": roster.get("lastName"),
            "team": roster.get("team"),
            "height": roster.get("height"),
            "weight": roster.get("weight"),
            "jersey": roster.get("jersey"),
            "position": roster.get("position"),
            "home_city": roster.get("homeCity"),
            "home_state": roster.get("homeState"),
            "home_country": roster.get("homeCountry"),
            "home_latitude": roster.get("homeLatitude"),
            "home_longitude": roster.get("homeLongitude"),
            "home_county_FIPS": roster.get("homeCountyFIPS"),
            #"recruit_ids": roster.get("recruitIds")
            "season": year,
        }

@dlt.source
def cfb_roster(api_key: str, year: int):
    yield cfb_roster_resource(api_key, year)
