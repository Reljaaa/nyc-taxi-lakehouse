-- What are the top 10 busiest origin-destination routes and their average trip duration?
SELECT
    pz.zone_name                                   AS pickup_zone,
    dz.zone_name                                   AS dropoff_zone,
    COUNT(*)                                       AS trip_count,
    ROUND(AVG(f.trip_duration_seconds) / 60.0, 1)  AS avg_duration_minutes,
    ROUND(AVG(f.trip_distance), 2)                 AS avg_distance_miles,
    ROUND(AVG(f.fare_amount), 2)                   AS avg_fare
FROM gold.default.fct_trips f
JOIN gold.default.dim_zones pz ON f.pickup_zone_sk  = pz.zone_sk
JOIN gold.default.dim_zones dz ON f.dropoff_zone_sk = dz.zone_sk
WHERE f.pickup_zone_sk  != 0
  AND f.dropoff_zone_sk != 0
GROUP BY pz.zone_name, dz.zone_name
ORDER BY trip_count DESC
LIMIT 10
