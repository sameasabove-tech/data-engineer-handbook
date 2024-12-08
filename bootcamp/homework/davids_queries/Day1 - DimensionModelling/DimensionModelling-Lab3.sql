-- In this lab, we build a network of NBA players and see who they play against/with the most and for which teams!

CREATE TYPE vertex_type as ENUM('player', 'team', 'game');

CREATE TABLE vertices (
    identifier TEXT,
    type vertex_type,
    properties JSON,
    PRIMARY KEY (identifier, type)
);

CREATE TYPE edge_type as ENUM('plays_against', 'shares_team', 'plays_in', 'plays_on');

CREATE TABLE edges (
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (subject_identifier, subject_type, object_identifier, object_type, edge_type)
);

INSERT INTO vertices
SELECT game_id as identifier
    , 'game'::vertex_type as type
    , json_build_object('pts_home', pts_home, 'pts_away', pts_away, 'winning_team', CASE WHEN home_team_wins=1 THEN home_team_id END) as properties
FROM games;

INSERT INTO vertices
WITH player_agg as (
    SELECT player_id as identifier
        , MAX(player_name) as player_name
        , count(1) as number_of_games
        , SUM(pts) as total_points
        , ARRAY_AGG(DISTINCT team_id) as teams
    FROM game_details
    GROUP BY player_id
)

SELECT identifier
    , 'player'::vertex_type
    , json_build_object(
        'player_name', player_name,
        'number_of_games', number_of_games,
        'total_points', total_points,
        'teams', teams
        )
FROM player_agg;

-- SELECT * from vertices;
INSERT INTO vertices
WITH teams_deduped AS (
    SELECT *
        , ROW_NUMBER() over (PARTITION BY team_id) as row_num
    FROM teams
)

SELECT team_id as identifier
    , 'team'::vertex_type as type
    , json_build_object(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'areana', arena
        'year_founded', yearfounded
        )
FROM teams_deduped
    WHERE row_num=1;

SELECT type, COUNT(*) from vertices
GROUP BY type;

INSERT INTO edges
WITH deduped AS (
    SELECT *, row_number() over (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
),
     filtered AS (
         SELECT * FROM deduped
         WHERE row_num = 1
     ),
     aggregated AS (
          SELECT
           f1.player_id as subject_player_id,
           f2.player_id as object_player_id,
           MAX(f1.player_name) as subject_player_name,
           MAX(f2.player_name) as object_player_name,
           CASE WHEN f1.team_abbreviation = f2.team_abbreviation
                THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
            END as edge_type,
            COUNT(1) AS num_games,
            SUM(f1.pts) AS subject_points,
            SUM(f2.pts) as object_points
        FROM filtered f1
            JOIN filtered f2
            ON f1.game_id = f2.game_id
            AND f1.player_name <> f2.player_name
        WHERE f1.player_id > f2.player_id
        GROUP BY
                f1.player_id,
           f2.player_id,
           CASE WHEN f1.team_abbreviation =         f2.team_abbreviation
                THEN  'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
            END
     )
SELECT subject_player_id as subject_identifier
    , 'player'::vertex_type as subject_type
    , object_player_id as object_identifer
    , 'player'::vertex_type as object_type
    , edge_type as edge_type
    , json_build_object('num_games', num_games
        , 'subject_points', subject_points
        , 'object_points', object_points)
FROM aggregated;

SELECT
    v.properties->>'player_name'
    , e.object_identifier
    , CAST(v.properties->>'number_of_games' as REAL)/
        CASE WHEN CAST(v.properties->>'total_points' AS REAL) = 0 THEN 1 ELSE CAST(v.properties->>'total_points' AS REAL) END
    , e.properties->>'subject_points'
    , e.properties->>'num_games'
FROM vertices v
    JOIN edges e on v.identifier = e.subject_identifier
        AND e.subject_type=v.type
WHERE e.object_type='player'::vertex_type;

INSERT INTO edges
with deduped as (
    SELECT *
        , ROW_NUMBER() over (PARTITION BY team_id) as row_num
    FROM game_details
)
SELECT player_id as subject_identifier
    , 'player'::vertex_type as subject_type
    , game_id as object_identifier
    , 'game'::vertex_type as object_type
    , 'plays_in'::edge_type as edge_type
    , json_build_array('start_position', start_position
        , 'pts', pts
        , 'team_id', team_id
        , 'team_abbreviation', team_abbreviation
    ) as properties
FRom deduped
    WHERE row_num=1;






SELECT -- *
    v.properties->>'player_name'
    , MAX(e.properties->>'pts')
FROM vertices v
    JOIN edges e on v.identifier = e.subject_identifier
        AND e.subject_type=v.type
GROUP BY 1
ORDER BY 2 DESC;
