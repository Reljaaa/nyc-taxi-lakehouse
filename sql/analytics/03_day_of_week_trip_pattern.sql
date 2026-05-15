-- Which day of the week has the highest average trip demand? (day_of_week: 1=Monday, 7=Sunday)
SELECT
    day_of_week,
    CASE day_of_week
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END                       AS day_name,
    ROUND(SUM(avg_trips), 1)  AS total_avg_demand,
    ROUND(AVG(avg_fare), 2)   AS avg_fare
FROM gold.default.hourly_demand_patterns
GROUP BY day_of_week
ORDER BY total_avg_demand DESC
