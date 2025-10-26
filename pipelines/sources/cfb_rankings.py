import dlt
import requests

@dlt.resource(
    name="cfb_rankings",
    primary_key=["season", "season_type", "week", "poll", "team_id"],
    write_disposition="merge"
)
def cfb_rankings_resource(api_key: str, year: int):
    url = "https://api.collegefootballdata.com/rankings"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"year": year}

    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()

    if not data:
        print(f"⚠️ No rankings data found for {year}")
        return

    for week_data in data:
        season = week_data.get("season")
        season_type = week_data.get("seasonType")
        week = week_data.get("week")

        for poll_entry in week_data.get("polls", []):
            poll_name = poll_entry.get("poll")

            for rank_entry in poll_entry.get("ranks", []):
                yield {
                    "season": season,
                    "season_type": season_type,
                    "week": week,
                    "poll": poll_name,
                    "rank": rank_entry.get("rank"),
                    "team_id": rank_entry.get("teamId"),
                    "school": rank_entry.get("school"),
                    "conference": rank_entry.get("conference"),
                    "first_place_votes": rank_entry.get("firstPlaceVotes"),
                    "points": rank_entry.get("points"),
                    "year": year,
                }

@dlt.source
def cfb_rankings(api_key: str, year: int):
    yield cfb_rankings_resource(api_key, year)
