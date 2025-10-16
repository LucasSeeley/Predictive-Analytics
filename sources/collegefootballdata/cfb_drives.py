import dlt
import requests

@dlt.resource(
    name="cfb_drives",
    primary_key="id",  # API's unique id for each drive
    write_disposition="merge"
)
def cfb_drives_resource(api_key: str, year: int, max_weeks: int = 18):
    """
    Fetch all drives for games in a season, iterating week by week.
    """
    url = "https://api.collegefootballdata.com/drives"
    headers = {"Authorization": f"Bearer {api_key}"}

    for week in range(1, max_weeks + 1):
        params = {"year": year, "week": week}
        try:
            resp = requests.get(url, headers=headers, params=params)
            resp.raise_for_status()
            drives = resp.json()
            if not drives:
                print(f"Week {week}: no drives found.")
                continue

            print(f"Week {week}: fetched {len(drives)} drives")
            # Yield drives individually
            for drive in drives:
                yield drive

        except requests.HTTPError:
            print(f"⚠️ Skipping week {week} for drives: {resp.status_code} {resp.text}")
            continue

@dlt.source
def cfb_drives(api_key: str, year: int):
    return cfb_drives_resource(api_key, year)
