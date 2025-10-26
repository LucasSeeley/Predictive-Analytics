MODEL (
    name cfb.cfb_roster,
    kind FULL
);
SELECT DISTINCT
    id AS player_id,
    season,
    CONCAT(first_name, ' ', last_name) AS player_name,
    team AS team_name,
    height,
    weight,
    jersey AS jersey_number,
    position,
    home_city,
    home_state,
    home_country,
    home_latitude,
    home_longitude
FROM cfb.cfb_roster_source