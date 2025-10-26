MODEL (
    name cfb.cfb_drives,
    kind FULL
);
SELECT DISTINCT
    id AS drive_id,
    game_id,
    year AS season,
    week,
    offense AS offense_team_name,
    defense AS defense_team_name,
    drive_number,
    scoring,
    drive_result,
    plays,
    yards,
    start_period,
    end_period,
    start_yardline,
    end_yardline,
    start_yards_to_goal,
    end_yards_to_goal,
    start_offense_score,
    start_defense_score,
    end_offense_score,
    end_defense_score,
    start_time_minutes,
    start_time_seconds,
    end_time_minutes,
    end_time_seconds,
    elapsed_minutes,
    elapsed_seconds
FROM cfb.cfb_drives_source