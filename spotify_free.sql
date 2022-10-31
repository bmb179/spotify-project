--Table creation
CREATE TABLE spotify_free_import (quarter_year text, quarter_dates date, active_free_users_mills numeric);

--Manually adding Q4 2014 stats as a starting point (first value will be null in the final analysis)
INSERT INTO spotify_free_import (quarter_year, active_free_users_mills)
VALUES ('Q4 2014', '60');

--Importing dataset from Statista
COPY spotify_free_import (quarter_year, active_free_users_mills)
FROM 'C:\directory\spotify_free_active_users_2015-2022.csv' 
DELIMITER ',';

--Cleaning up the dates using native Postgres function regexp_replace()
UPDATE spotify_free_import
SET quarter_dates = CASE WHEN quarter_year ~* 'Q1' THEN CAST(regexp_replace(quarter_year, '^..' , 'March 31,') AS date)
                         WHEN quarter_year ~* 'Q2' THEN CAST(regexp_replace(quarter_year, '^..' , 'June 30,') AS date)
                         WHEN quarter_year ~* 'Q3' THEN CAST(regexp_replace(quarter_year, '^..' , 'September 30,') AS date)
                         WHEN quarter_year ~* 'Q4' THEN CAST(regexp_replace(quarter_year, '^..' , 'December 31,') AS date) END;

--Querying statistics of free monthly active user growth
--NOTE: It was decided to make these calculations in the query instead of in the table design since it would be
-- better-optimized to be updated on a quarterly basis 
WITH spotify_stats AS (
    SELECT avg(qtly_change) AS mean_qtly_change,
           stddev(qtly_change) AS stddev_qtly_change,
           avg(qtly_change_rate) AS mean_qtly_change_rate,
           stddev(qtly_change_rate) AS stddev_qtly_change_rate
    FROM (SELECT (active_free_users_mills - lag(active_free_users_mills) OVER (ORDER BY quarter_dates))
                    /lag(active_free_users_mills) OVER (ORDER BY quarter_dates) 
                    AS qtly_change_rate,
                 active_free_users_mills - lag(active_free_users_mills) OVER (ORDER BY quarter_dates)  
                    AS qtly_change
          FROM spotify_free_import) AS change_rate_table, spotify_free_import
                      )
SELECT quarter_year AS quarter,
       quarter_dates AS quarter_end, 
       active_free_users_mills,
       active_free_users_mills - lag(active_free_users_mills) OVER (ORDER BY quarter_dates) 
           AS qtly_change,
       CAST((active_free_users_mills - lag(active_free_users_mills) OVER (ORDER BY quarter_dates) - mean_qtly_change)
           /stddev_qtly_change AS numeric(4,3))
           AS zscore_qtly_change,
       CAST((active_free_users_mills - lag(active_free_users_mills) OVER (ORDER BY quarter_dates))
           /lag(active_free_users_mills) OVER (ORDER BY quarter_dates) AS numeric(4,3)) 
           AS qtly_change_rate,
       CAST(((active_free_users_mills - lag(active_free_users_mills) OVER (ORDER BY quarter_dates))
           /lag(active_free_users_mills) OVER (ORDER BY quarter_dates) - mean_qtly_change_rate)
           /stddev_qtly_change_rate AS numeric(4,3)) 
           AS zscore_qtly_change_rate
FROM spotify_free_import, spotify_stats
ORDER BY quarter_dates ASC;

--Export
COPY (
WITH spotify_stats AS (
    SELECT avg(qtly_change) AS mean_qtly_change,
           stddev(qtly_change) AS stddev_qtly_change,
           avg(qtly_change_rate) AS mean_qtly_change_rate,
           stddev(qtly_change_rate) AS stddev_qtly_change_rate
    FROM (SELECT (active_free_users_mills - lag(active_free_users_mills) OVER (ORDER BY quarter_dates))
                    /lag(active_free_users_mills) OVER (ORDER BY quarter_dates) 
                    AS qtly_change_rate,
                 active_free_users_mills - lag(active_free_users_mills) OVER (ORDER BY quarter_dates)  
                    AS qtly_change
          FROM spotify_free_import) AS change_rate_table, spotify_free_import
                      )
SELECT quarter_year AS quarter,
       quarter_dates AS quarter_end, 
       active_free_users_mills,
       active_free_users_mills - lag(active_free_users_mills) OVER (ORDER BY quarter_dates) 
           AS qtly_change,
       CAST((active_free_users_mills - lag(active_free_users_mills) OVER (ORDER BY quarter_dates) - mean_qtly_change)
           /stddev_qtly_change AS numeric(4,3))
           AS zscore_qtly_change,
       CAST((active_free_users_mills - lag(active_free_users_mills) OVER (ORDER BY quarter_dates))
           /lag(active_free_users_mills) OVER (ORDER BY quarter_dates) AS numeric(4,3)) 
           AS qtly_change_rate,
       CAST(((active_free_users_mills - lag(active_free_users_mills) OVER (ORDER BY quarter_dates))
           /lag(active_free_users_mills) OVER (ORDER BY quarter_dates) - mean_qtly_change_rate)
           /stddev_qtly_change_rate AS numeric(4,3)) 
           AS zscore_qtly_change_rate
FROM spotify_free_import, spotify_stats
ORDER BY quarter_dates ASC
)
TO 'C:\directory\spotify_free_analysis_15-22.csv' 
WITH (FORMAT CSV, HEADER);