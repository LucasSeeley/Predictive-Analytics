MODEL (
    name cfb.cfb_stadiums,
    kind FULL
);
SELECT DISTINCT
    location_id AS stadium_id,
    stadium_name AS stadium_name,
    city,
    state,
    zip AS zip_code,
    country_code,
    timezone,
    latitude,
    longitude,
    elevation AS stadium_elevation,
    capacity AS stadium_capacity,
    construction_year,
    grass AS stadium_is_grass,
    dome AS stadium_is_dome
FROM cfb.cfb_teams_source