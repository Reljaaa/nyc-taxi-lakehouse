-- How does trip demand and revenue compare between weekdays and weekends?
-- Uses daily_metrics_by_zone to compare per-day averages (not totals).
WITH daily_totals AS (
    SELECT
        pickup_date_sk,
        CASE WHEN day_of_week IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
        SUM(total_trips)   AS day_trips,
        SUM(total_revenue) AS day_revenue
    FROM gold.default.daily_metrics_by_zone
    GROUP BY pickup_date_sk, day_of_week
)
SELECT
    day_type,
    COUNT(*)                    AS num_days,
    ROUND(AVG(day_trips), 0)    AS avg_trips_per_day,
    ROUND(AVG(day_revenue), 2)  AS avg_revenue_per_day
FROM daily_totals
GROUP BY day_type
ORDER BY avg_trips_per_day DESC
