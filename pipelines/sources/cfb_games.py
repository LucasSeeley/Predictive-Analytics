import dlt
import requests

@dlt.resource(
    name="cfb_games",
    primary_key="id",
    write_disposition="merge"
)
def cfb_games_resource(api_key: str, year: int):
    url = "https://api.collegefootballdata.com/games"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"year": year}

    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()

    for game in data:
        yield {
            "id": game.get("id"),
            "season": game.get("season"),
            "week": game.get("week"),
            "season_type": game.get("seasonType"),
            "start_date": game.get("startDate"),
            "start_time_tbd": game.get("startTimeTBD"),
            "completed": game.get("completed"),
            "neutral_site": game.get("neutralSite"),
            "conference_game": game.get("conferenceGame"),
            "attendance": game.get("attendance"),
            "venue_id": game.get("venueId"),
            "venue": game.get("venue"),
            "home_id": game.get("homeId"),
            "home_team": game.get("homeTeam"),
            "home_conference": game.get("homeConference"),
            "home_classification": game.get("homeClassification"),
            "home_points": game.get("homePoints"),
            "home_line_scores": game.get("homeLineScores"),
            "home_postgame_win_probability": game.get("homePostgameWinProbability"),
            "home_pregame_elo": game.get("homePregameElo"),
            "home_postgame_elo": game.get("homePostgameElo"),
            "away_id": game.get("awayId"),
            "away_team": game.get("awayTeam"),
            "away_conference": game.get("awayConference"),
            "away_classification": game.get("awayClassification"),
            "away_points": game.get("awayPoints"),
            "away_line_scores": game.get("awayLineScores"),
            "away_postgame_win_probability": game.get("awayPostgameWinProbability"),
            "away_pregame_elo": game.get("awayPregameElo"),
            "away_postgame_elo": game.get("awayPostgameElo"),
            "excitement_index": game.get("excitementIndex"),
            "highlights": game.get("highlights"),
            "notes": game.get("notes")
        }

@dlt.source
def cfb_games(api_key: str, year: int):
    yield cfb_games_resource(api_key, year)
