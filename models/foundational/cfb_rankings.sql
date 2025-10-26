MODEL (
    name cfb.cfb_rankings,
    kind FULL
);
SELECT
    season,
    season_type,
    week,
    team_id,
    poll,
    rank,
    first_place_votes,
    points
FROM cfb.cfb_rankings_source