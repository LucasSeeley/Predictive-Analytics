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
    spread AS current_spread,
    over_under_open,
    over_under AS current_over_under
FROM cfb.cfb_lines_source