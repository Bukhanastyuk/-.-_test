----Задание 1----

WITH ordered_sessions AS (
    SELECT *,
           LAG(end_time) OVER (PARTITION BY player_id ORDER BY start_time) as prev_end_time
    FROM game_sessions
),

session_groups AS (
    SELECT *,
           CASE WHEN EXTRACT(EPOCH FROM (start_time - prev_end_time)) / 60 > 5 THEN 1
                ELSE 0
           END as is_new_session
    FROM ordered_sessions
),

marked_sessions AS (
    SELECT *,
           SUM(is_new_session) OVER (PARTITION BY player_id ORDER BY start_time) as session_group
    FROM session_groups
),

final_sessions AS (
SELECT player_id,
       country,
       MIN(start_time) as session_start,
       MAX(end_time) as session_end,
    SUM(EXTRACT(EPOCH FROM (end_time - start_time)) / 60) as session_duration
FROM marked_sessions
GROUP BY player_id, country, session_group
ORDER BY player_id, session_start),

daily_sessions AS (
    SELECT player_id,
           country,
           DATE_TRUNC('day', session_start) as day,
           SUM(session_duration) as duration_of_all_sessions,
           MIN(session_duration) as shortest_session,
           MAX(session_duration) as longest_session
    FROM final_sessions
    GROUP BY player_id, country, day
),

ranked_players AS (
    SELECT *,
           RANK() OVER (PARTITION BY country, day ORDER BY duration_of_all_sessions DESC) as rank_of_the_user
    FROM daily_sessions
)
SELECT * FROM ranked_players
ORDER BY country, day, rank_of_the_user;


----Задание 2----

WITH session_time AS (
SELECT
    player_id,
    start_time :: date AS session_date,
    start_time,
    end_time,
    EXTRACT(EPOCH FROM (end_time - start_time)) / 60 AS session_time
  FROM game_sessions
)

SELECT
  player_id,
  session_date,
  start_time,
  end_time,
  SUM(session_time) OVER (PARTITION BY player_id ORDER BY start_time) AS agg_session_time
FROM session_time
ORDER BY player_id, session_date, start_time;
