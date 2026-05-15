-- How do trip volume and revenue trend month over month in 2024?
SELECT
    year,
    month,
    SUM(total_trips)              AS total_trips,
    ROUND(SUM(total_revenue), 2)  AS total_revenue,
    ROUND(AVG(avg_fare), 2)       AS avg_fare,
    ROUND(AVG(avg_distance), 2)   AS avg_distance_miles
FROM gold.default.daily_metrics_by_zone
GROUP BY year, month
ORDER BY year, month
