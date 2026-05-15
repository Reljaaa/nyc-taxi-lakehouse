-- What is the average fare per mile by pickup borough?
SELECT
    dz.borough,
    COUNT(*)                                         AS trip_count,
    ROUND(AVG(f.fare_amount), 2)                     AS avg_fare,
    ROUND(AVG(f.trip_distance), 2)                   AS avg_distance_miles,
    ROUND(AVG(f.fare_amount / f.trip_distance), 2)   AS avg_fare_per_mile
FROM gold.default.fct_trips f
JOIN gold.default.dim_zones dz
    ON f.pickup_zone_sk = dz.zone_sk
WHERE f.trip_distance > 0
  AND f.pickup_zone_sk != 0
GROUP BY dz.borough
ORDER BY avg_fare_per_mile DESC
