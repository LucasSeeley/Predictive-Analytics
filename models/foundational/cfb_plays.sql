MODEL (
    name cfb.cfb_plays,
    kind FULL
);
SELECT DISTINCT
    id AS play_id,
    drive_id,
    game_id,
    drive_number,
    play_number,
    period AS quarter,
    clock_minutes,
    clock_seconds,
    offense_timeouts,
    defense_timeouts,
    yardline,
    yards_to_goal,
    down,
    distance AS distance_to_first_down,
    yards_gained,
    scoring,
    play_type,
    play_text
FROM cfb.cfb_plays_source