import dlt
import requests

@dlt.resource(
    name="cfb_teams_source",
    primary_key="id",
    write_disposition="merge"
)
def cfb_teams_resource(api_key: str, year: int):
    url = "https://api.collegefootballdata.com/teams"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"year": year}

    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()

    for team in data:
        location = team.get("location") or {}
        yield {
            "id": team.get("id"),
            "school": team.get("school"),
            "mascot": team.get("mascot"),
            "abbreviation": team.get("abbreviation"),
            #"alternate_names": team.get("alternateNames"),
            "conference": team.get("conference"),
            "division": team.get("division"),
            "classification": team.get("classification"),
            "color": team.get("color"),
            "alternate_color": team.get("alternateColor"),
            #"logos": team.get("logos"),
            "twitter": team.get("twitter"),
            "season": year,

            # Flatten location info
            "location_id": location.get("id"),
            "stadium_name": location.get("name"),
            "city": location.get("city"),
            "state": location.get("state"),
            "zip": location.get("zip"),
            "country_code": location.get("countryCode"),
            "timezone": location.get("timezone"),
            "latitude": location.get("latitude"),
            "longitude": location.get("longitude"),
            "elevation": location.get("elevation"),
            "capacity": location.get("capacity"),
            "construction_year": location.get("constructionYear"),
            "grass": location.get("grass"),
            "dome": location.get("dome"),
        }

@dlt.source
def cfb_teams(api_key: str, year: int):
    yield cfb_teams_resource(api_key, year)
