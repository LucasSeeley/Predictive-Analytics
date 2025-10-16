import dlt
import requests

@dlt.resource(
    name="cfb_plays",
    primary_key="id",  # API's unique id for each play
    write_disposition="merge"
)
def cfb_plays_resource(api_key: str, year: int, max_weeks: int = 18):
    """
    Fetch all plays for games in a season, iterating week by week.
    """
    url = "https://api.collegefootballdata.com/plays"
    headers = {"Authorization": f"Bearer {api_key}"}

    for week in range(1, max_weeks + 1):
        params = {"year": year, "week": week}
        try:
            resp = requests.get(url, headers=headers, params=params)
            resp.raise_for_status()
            plays = resp.json()
            if not plays:
                print(f"Week {week}: no plays found.")
                continue

            print(f"Week {week}: fetched {len(plays)} plays")
            # Yield plays individually
            for play in plays:
                yield play

        except requests.HTTPError:
            print(f"⚠️ Skipping week {week} for plays: {resp.status_code} {resp.text}")
            continue

@dlt.source
def cfb_plays(api_key: str, year: int):
    return cfb_plays_resource(api_key, year)
