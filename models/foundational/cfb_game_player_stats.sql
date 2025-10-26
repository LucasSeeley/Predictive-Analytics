MODEL (
    name cfb.cfb_game_player_stats,
    kind FULL
);
SELECT DISTINCT
    game_id,
    athlete_id AS player_id,
    team AS team_name,
    season,
    week,
    category_name AS stat_category,
    type_name AS stat_type,
    stat AS player_stat
FROM cfb.cfb_game_players_source