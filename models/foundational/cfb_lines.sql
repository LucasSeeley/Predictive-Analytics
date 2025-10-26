MODEL (
    name cfb.cfb_lines,
    kind FULL
);
SELECT DISTINCT
    id AS line_id,
    season,
    season_type,
    week,
    home_team_id AS home_id,
    away_team_id AS away_id,
    provider AS line_provider,
    home_moneyline,
    away_moneyline,
    spread_open,
    spread AS spread_close,
    over_under_open,
    over_under AS over_under_close
FROM cfb.cfb_lines_source