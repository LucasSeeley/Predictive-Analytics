import dlt
import requests

@dlt.resource(
    name="cfb_drives",
    primary_key="id",
    write_disposition="merge"
)
def cfb_drives_resource(api_key: str, year: int, max_weeks: int = 18):
    url = "https://api.collegefootballdata.com/drives"
    headers = {"Authorization": f"Bearer {api_key}"}

    for week in range(1, max_weeks + 1):
        params = {"year": year, "week": week}
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"⚠️ Skipping week {week}: {resp.status_code} {resp.text}")
            continue

        drives = resp.json()
        if not drives:
            continue

        for drive in drives:
            start_time = drive.get("startTime", {}) or {}
            end_time = drive.get("endTime", {}) or {}
            elapsed = drive.get("elapsed", {}) or {}

            yield {
                "id": drive.get("id"),
                "game_id": drive.get("gameId"),
                "offense": drive.get("offense"),
                "offense_conference": drive.get("offenseConference"),
                "defense": drive.get("defense"),
                "defense_conference": drive.get("defenseConference"),
                "is_home_offense": drive.get("isHomeOffense"),
                "drive_number": drive.get("driveNumber"),
                "scoring": drive.get("scoring"),
                "drive_result": drive.get("driveResult"),
                "plays": drive.get("plays"),
                "yards": drive.get("yards"),
                "start_period": drive.get("startPeriod"),
                "end_period": drive.get("endPeriod"),
                "start_yardline": drive.get("startYardline"),
                "end_yardline": drive.get("endYardline"),
                "start_yards_to_goal": drive.get("startYardsToGoal"),
                "end_yards_to_goal": drive.get("endYardsToGoal"),
                "start_offense_score": drive.get("startOffenseScore"),
                "start_defense_score": drive.get("startDefenseScore"),
                "end_offense_score": drive.get("endOffenseScore"),
                "end_defense_score": drive.get("endDefenseScore"),
                # Flattened nested dicts
                "start_time_minutes": start_time.get("minutes"),
                "start_time_seconds": start_time.get("seconds"),
                "end_time_minutes": end_time.get("minutes"),
                "end_time_seconds": end_time.get("seconds"),
                "elapsed_minutes": elapsed.get("minutes"),
                "elapsed_seconds": elapsed.get("seconds"),
                # Metadata
                "year": year,
                "week": week,
            }

@dlt.source
def cfb_drives(api_key: str, year: int):
    yield cfb_drives_resource(api_key, year)
