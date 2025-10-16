# sources/collegefootballdata/cfb_rankings.py
import dlt
import requests

@dlt.resource(
    name="cfb_rankings",
    primary_key=["season", "seasonType", "week", "poll", "teamId"],  # composite key
    write_disposition="merge"
)
def cfb_rankings_resource(api_key: str, year: int):
    """
    Fetch all team rankings for a season using the /rankings endpoint.
    Only the 'year' parameter is required.
    Flattens nested polls/ranks structure for DLT ingestion.
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"year": year}

    try:
        resp = requests.get(
            "https://api.collegefootballdata.com/rankings",
            headers=headers,
            params=params
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            print(f"No rankings data found for {year}.")
            return

        flat_rankings = []
        for week_data in data:
            season = week_data.get("season")
            seasonType = week_data.get("seasonType")
            week = week_data.get("week")
            polls = week_data.get("polls", [])
            for poll_entry in polls:
                poll_name = poll_entry.get("poll")
                ranks = poll_entry.get("ranks", [])
                for rank_entry in ranks:
                    flat_rankings.append({
                        "season": season,
                        "seasonType": seasonType,
                        "week": week,
                        "poll": poll_name,
                        "rank": rank_entry.get("rank"),
                        "teamId": rank_entry.get("teamId"),
                        "school": rank_entry.get("school"),
                        "conference": rank_entry.get("conference"),
                        "firstPlaceVotes": rank_entry.get("firstPlaceVotes"),
                        "points": rank_entry.get("points")
                    })

        print(f"Fetched {len(flat_rankings)} ranking records for {year}")
        yield from flat_rankings

    except requests.HTTPError:
        print(f"⚠️ Failed to fetch rankings for {year}: {resp.status_code} {resp.text}")
    except requests.exceptions.JSONDecodeError:
        print(f"⚠️ Invalid JSON returned for year {year}: {resp.text}")

@dlt.source
def cfb_rankings(api_key: str, year: int):
    return cfb_rankings_resource(api_key, year)
