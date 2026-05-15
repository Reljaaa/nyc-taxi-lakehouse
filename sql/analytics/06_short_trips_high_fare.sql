-- Are there suspiciously short trips with high fares after silver DQ filtering? (data quality signal)
SELECT
    dz.borough,
    COUNT(*)                                       AS trip_count,
    ROUND(AVG(f.fare_amount), 2)                   AS avg_fare,
    ROUND(AVG(f.trip_distance), 3)                 AS avg_distance_miles,
    ROUND(AVG(f.trip_duration_seconds) / 60.0, 1)  AS avg_duration_minutes
FROM gold.default.fct_trips f
JOIN gold.default.dim_zones dz
    ON f.pickup_zone_sk = dz.zone_sk
WHERE f.trip_distance < 0.5
  AND f.fare_amount > 20.0
  AND f.pickup_zone_sk != 0
GROUP BY dz.borough
ORDER BY trip_count DESC
