import dlt
import requests

@dlt.resource(
    name="cfb_game_players_source",
    primary_key=("game_id", "team", "athlete_id", "category_name", "type_name"),
    write_disposition="merge"
)
def cfb_game_players_resource(api_key: str, year: int, max_weeks: int = 18):
    url = "https://api.collegefootballdata.com/games/players"
    headers = {"Authorization": f"Bearer {api_key}"}

    for week in range(1, max_weeks + 1):
        params = {"year": year, "week": week}
        resp = requests.get(url, headers=headers, params=params)

        if resp.status_code != 200:
            print(f"⚠️ Skipping week {week}: {resp.status_code} {resp.text}")
            continue

        data = resp.json()
        if not data:
            continue

        for game in data:
            game_id = game.get("id")

            for team_entry in game.get("teams", []):
                team = team_entry.get("team")
                conference = team_entry.get("conference")
                home_away = team_entry.get("homeAway")
                points = team_entry.get("points")

                for category in team_entry.get("categories", []):
                    category_name = category.get("name")

                    for type_entry in category.get("types", []):
                        type_name = type_entry.get("name")

                        for athlete in type_entry.get("athletes", []):
                            yield {
                                "game_id": game_id,
                                "team": team,
                                "conference": conference,
                                "home_away": home_away,
                                "points": points,
                                "category_name": category_name,
                                "type_name": type_name,
                                "athlete_id": athlete.get("id"),
                                "athlete_name": athlete.get("name"),
                                "stat": athlete.get("stat"),
                                "season": year,
                                "week": week,
                            }

@dlt.source
def cfb_game_players(api_key: str, year: int):
    yield cfb_game_players_resource(api_key, year)
