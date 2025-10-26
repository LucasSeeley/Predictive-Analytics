import dlt
import requests

@dlt.resource(
    name="cfb_lines",
    primary_key=["id", "provider"],
    write_disposition="merge"
)
def cfb_lines_resource(api_key: str, year: int):
    url = "https://api.collegefootballdata.com/lines"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"year": year}

    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()

    for game in data:
        base = {
            "id": game.get("id"),
            "season": game.get("season"),
            "season_type": game.get("seasonType"),
            "week": game.get("week"),
            "start_date": game.get("startDate"),
            "home_team_id": game.get("homeTeamId"),
            "home_team": game.get("homeTeam", "unknown"),
            "home_conference": game.get("homeConference"),
            "home_classification": game.get("homeClassification"),
            "home_score": game.get("homeScore"),
            "away_team_id": game.get("awayTeamId"),
            "away_team": game.get("awayTeam", "unknown"),
            "away_conference": game.get("awayConference"),
            "away_classification": game.get("awayClassification"),
            "away_score": game.get("awayScore"),
        }

        for line in game.get("lines") or []:
            yield {
                **base,
                "provider": line.get("provider", "unknown"),
                "spread": line.get("spread"),
                "formatted_spread": line.get("formattedSpread"),
                "spread_open": line.get("spreadOpen"),
                "over_under": line.get("overUnder"),
                "over_under_open": line.get("overUnderOpen"),
                "home_moneyline": line.get("homeMoneyline"),
                "away_moneyline": line.get("awayMoneyline"),
            }


@dlt.source
def cfb_lines(api_key: str, year: int):
    yield cfb_lines_resource(api_key, year)
