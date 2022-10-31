SELECT spotify_free_query.quarter,
       spotify_free_query.quarter_end,
       CAST(spotify_prem_query.prem_subs_in_mills/
            (spotify_free_query.active_free_users_mills + spotify_prem_query.prem_subs_in_mills)
            AS numeric(4,3)) AS prem_subs_as_proportion_of_total,
       spotify_free_query.zscore_qtly_change AS free_qtly_change_zscore,
       spotify_free_query.zscore_qtly_change_rate AS free_qtly_change_rate_zscore,
       spotify_prem_query.zscore_qtly_change AS prem_qtly_change_zscore,
       spotify_prem_query.zscore_qtly_change_rate AS prem_qtly_change_rate_zscore
FROM (
--START OF THE FIRST SUBQUERY IN THE JOIN--
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
--END OF THE FIRST SUBQUERY IN THE JOIN--
) AS spotify_free_query INNER JOIN (
--START OF THE SECOND SUBQUERY IN THE JOIN--
WITH spotify_stats AS (
    SELECT avg(qtly_change) AS mean_qtly_change,
           stddev(qtly_change) AS stddev_qtly_change,
           avg(qtly_change_rate) AS mean_qtly_change_rate,
           stddev(qtly_change_rate) AS stddev_qtly_change_rate
    FROM (SELECT (prem_subs_in_mills - lag(prem_subs_in_mills) OVER (ORDER BY quarter_dates))
                    /lag(prem_subs_in_mills) OVER (ORDER BY quarter_dates)
                    AS qtly_change_rate,
                 prem_subs_in_mills - lag(prem_subs_in_mills) 
                    OVER (ORDER BY quarter_dates)  
                    AS qtly_change
          FROM spotify_prem_import) AS change_rate_table, spotify_prem_import
                      )
SELECT quarter_year AS quarter,
       quarter_dates AS quarter_end, 
       prem_subs_in_mills,
       prem_subs_in_mills - lag(prem_subs_in_mills) OVER (ORDER BY quarter_dates) 
           AS qtly_change,
       CAST((prem_subs_in_mills - lag(prem_subs_in_mills) OVER (ORDER BY quarter_dates) - mean_qtly_change)
           /stddev_qtly_change AS numeric(4,3))
           AS zscore_qtly_change,
       CAST((prem_subs_in_mills - lag(prem_subs_in_mills) OVER (ORDER BY quarter_dates))
           /lag(prem_subs_in_mills) OVER (ORDER BY quarter_dates) AS numeric(4,3)) 
           AS qtly_change_rate,
       CAST(((prem_subs_in_mills - lag(prem_subs_in_mills) OVER (ORDER BY quarter_dates))
           /lag(prem_subs_in_mills) OVER (ORDER BY quarter_dates) - mean_qtly_change_rate)
           /stddev_qtly_change_rate AS numeric(4,3)) 
           AS zscore_qtly_change_rate
FROM spotify_prem_import, spotify_stats
) AS spotify_prem_query ON spotify_free_query.quarter_end = spotify_prem_query.quarter_end
--END OF THE SECOND SUBQUERY IN THE JOIN--
WHERE spotify_free_query.quarter <> 'Q4 2014'
ORDER BY spotify_free_query.quarter_end ASC;

--EXPORT THIS TO SEE SIGNIFICANT Z-SCORES IN A TABLE
COPY (

)
TO 'C:\Users\Brian\Desktop\practical-sql-2-main\Spotify Project\spotify_relational_analysis_15-22.csv' 
WITH (FORMAT CSV, HEADER);