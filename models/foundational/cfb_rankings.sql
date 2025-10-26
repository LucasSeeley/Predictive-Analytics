MODEL (
    name cfb.cfb_rankings,
    kind FULL
);
SELECT DISTINCT
    season,
    season_type,
    week,
    team_id,
    poll,
    rank AS team_rank,
    first_place_votes,
    points AS poll_points
FROM cfb.cfb_rankings_source