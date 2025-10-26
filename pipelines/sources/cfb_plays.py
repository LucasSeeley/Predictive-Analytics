import dlt
import requests

@dlt.resource(
    name="cfb_plays_source",
    primary_key="id",
    write_disposition="merge"
)
def cfb_plays_resource(api_key: str, year: int, max_weeks: int = 18):
    url = "https://api.collegefootballdata.com/plays"
    headers = {"Authorization": f"Bearer {api_key}"}

    for week in range(1, max_weeks + 1):
        params = {"year": year, "week": week}
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"⚠️ Skipping week {week}: {resp.status_code} {resp.text}")
            continue

        plays = resp.json()
        if not plays:
            continue

        for play in plays:
            clock = play.get("clock", {}) or {}

            yield {
                "id": play.get("id"),
                "drive_id": play.get("driveId"),
                "game_id": play.get("gameId"),
                "drive_number": play.get("driveNumber"),
                "play_number": play.get("playNumber"),
                "offense": play.get("offense"),
                "offense_conference": play.get("offenseConference"),
                "offense_score": play.get("offenseScore"),
                "defense": play.get("defense"),
                "defense_conference": play.get("defenseConference"),
                "defense_score": play.get("defenseScore"),
                "home": play.get("home"),
                "away": play.get("away"),
                "period": play.get("period"),
                "clock_minutes": clock.get("minutes"),
                "clock_seconds": clock.get("seconds"),
                "offense_timeouts": play.get("offenseTimeouts"),
                "defense_timeouts": play.get("defenseTimeouts"),
                "yardline": play.get("yardline"),
                "yards_to_goal": play.get("yardsToGoal"),
                "down": play.get("down"),
                "distance": play.get("distance"),
                "yards_gained": play.get("yardsGained"),
                "scoring": play.get("scoring"),
                "play_type": play.get("playType"),
                "play_text": play.get("playText"),
                "ppa": play.get("ppa"),
                "wallclock": play.get("wallclock"),
                "year": year,
                "week": week,
            }

@dlt.source
def cfb_plays(api_key: str, year: int):
    yield cfb_plays_resource(api_key, year)
