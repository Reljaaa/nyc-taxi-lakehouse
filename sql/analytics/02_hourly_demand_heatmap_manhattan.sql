-- Which Manhattan zones and hours have the highest average trip demand?
SELECT
    pickup_hour,
    zone_name,
    ROUND(avg_trips, 2) AS avg_trips,
    ROUND(avg_fare, 2)  AS avg_fare
FROM gold.default.hourly_demand_patterns
WHERE borough = 'Manhattan'
ORDER BY avg_trips DESC
LIMIT 50
