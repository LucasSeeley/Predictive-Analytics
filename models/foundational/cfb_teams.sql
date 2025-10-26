MODEL (
    name cfb.cfb_teams,
    kind FULL
);
SELECT DISTINCT
    id AS team_id,
    school AS team_name,
    mascot AS team_mascot,
    conference AS conference,
    classification AS division,
    color AS team_color,
    alternate_color AS team_alternate_color,
    twitter AS team_twitter_handle,
    season AS season,
    location_id AS stadium_id
FROM cfb.cfb_teams_source