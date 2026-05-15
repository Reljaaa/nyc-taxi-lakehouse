-- Which are the top 10 pickup zones by total revenue in 2024?
SELECT
    zone_name,
    borough,
    SUM(total_trips)                                 AS total_trips,
    ROUND(SUM(total_revenue), 2)                     AS total_revenue,
    ROUND(SUM(total_revenue) / SUM(total_trips), 2)  AS avg_revenue_per_trip
FROM gold.default.daily_metrics_by_zone
GROUP BY zone_name, borough
ORDER BY total_revenue DESC
LIMIT 10
