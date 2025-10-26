MODEL (
    name cfb.cfb_games,
    kind FULL
);
SELECT DISTINCT
    id AS game_id,
    season,
    week,
    season_type,
    start_date,
    start_time_tbd,
    completed AS game_completed,
    neutral_site,
    conference_game,
    venue_id AS stadium_id,
    home_id,
    home_points,
    home_pregame_elo,
    home_postgame_elo,
    away_id,
    away_points,
    away_pregame_elo,
    away_postgame_elo,
    excitement_index
FROM cfb.cfb_games_source