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

    flat_lines = []

    for game in data:
        base = {
            "id": game.get("id"),
            "season": game.get("season"),
            "seasonType": game.get("seasonType"),
            "week": game.get("week"),
            "startDate": game.get("startDate"),
            "homeTeamId": game.get("homeTeamId"),
            "homeTeam": game.get("homeTeam"),
            "homeConference": game.get("homeConference"),
            "homeClassification": game.get("homeClassification"),
            "homeScore": game.get("homeScore"),
            "awayTeamId": game.get("awayTeamId"),
            "awayTeam": game.get("awayTeam"),
            "awayConference": game.get("awayConference"),
            "awayClassification": game.get("awayClassification"),
            "awayScore": game.get("awayScore"),
        }

        for line in game.get("lines") or []:
            flat_lines.append({
                **base,
                "provider": line.get("provider") or "unknown",
                "spread": line.get("spread"),
                "formattedSpread": line.get("formattedSpread"),
                "spreadOpen": line.get("spreadOpen"),
                "overUnder": line.get("overUnder"),
                "overUnderOpen": line.get("overUnderOpen"),
                "homeMoneyline": line.get("homeMoneyline"),
                "awayMoneyline": line.get("awayMoneyline"),
            })

    yield from flat_lines


@dlt.source
def cfb_lines(api_key: str, year: int):
    return cfb_lines_resource(api_key, year)
