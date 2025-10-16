import dlt
import requests
import time


@dlt.resource(
    name="cfb_games",
    primary_key="id",
    write_disposition="merge"
)
def cfb_games_resource(api_key: str, year: int):
    url = "https://api.collegefootballdata.com/games"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    params = {"year": year}

    print("Request params:", params)
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()
    print(f"Received {len(data)} games for {year}")
    yield from data



@dlt.source
def cfb_games(api_key: str, years: list[int]):
    """
    DLT source wrapper for College Football games data.
    """
    return cfb_games_resource(api_key, years)
