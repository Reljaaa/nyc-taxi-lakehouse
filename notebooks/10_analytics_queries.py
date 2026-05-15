# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Analytics Queries

# COMMAND ----------
# MAGIC %md
# MAGIC ## Q1: Top 10 Pickup Zones by Revenue

# COMMAND ----------

display(spark.sql("""-- Which are the top 10 pickup zones by total revenue in 2024?
SELECT
    zone_name,
    borough,
    SUM(total_trips)                                 AS total_trips,
    ROUND(SUM(total_revenue), 2)                     AS total_revenue,
    ROUND(SUM(total_revenue) / SUM(total_trips), 2)  AS avg_revenue_per_trip
FROM gold.default.daily_metrics_by_zone
GROUP BY zone_name, borough
ORDER BY total_revenue DESC
LIMIT 10"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Q2: Hourly Demand Heatmap for Manhattan

# COMMAND ----------

display(spark.sql("""-- Which Manhattan zones and hours have the highest average trip demand?
SELECT
    pickup_hour,
    zone_name,
    ROUND(avg_trips, 2) AS avg_trips,
    ROUND(avg_fare, 2)  AS avg_fare
FROM gold.default.hourly_demand_patterns
WHERE borough = 'Manhattan'
ORDER BY avg_trips DESC
LIMIT 50"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Q3: Day-of-Week Trip Pattern

# COMMAND ----------

display(spark.sql("""-- Which day of the week has the highest average trip demand? (day_of_week: 1=Monday, 7=Sunday)
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
ORDER BY total_avg_demand DESC"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Q4: Payment Type Distribution

# COMMAND ----------

display(spark.sql("""-- How are trips distributed by payment type, and how do tips differ across types?
SELECT
    dpt.payment_type_description,
    COUNT(*)                                                       AS trip_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)            AS pct_of_trips,
    ROUND(AVG(f.tip_amount), 2)                                    AS avg_tip_amount,
    ROUND(AVG(f.tip_percentage), 2)                                AS avg_tip_pct
FROM gold.default.fct_trips f
JOIN gold.default.dim_payment_type dpt
    ON f.payment_type_sk = dpt.payment_type_sk
GROUP BY dpt.payment_type_description
ORDER BY trip_count DESC"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Q5: Vendor Market Share

# COMMAND ----------

display(spark.sql("""-- How does vendor market share evolve month over month in 2024?
SELECT
    dd.year,
    dd.month,
    dv.vendor_name,
    COUNT(*)                                                                           AS trip_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY dd.year, dd.month), 2)  AS market_share_pct
FROM gold.default.fct_trips f
JOIN gold.default.dim_vendor dv
    ON f.vendor_sk = dv.vendor_sk
JOIN gold.default.dim_date dd
    ON f.pickup_date_sk = dd.date_sk
WHERE dv.vendor_id != 0
GROUP BY dd.year, dd.month, dv.vendor_name
ORDER BY dd.year, dd.month, trip_count DESC"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Q6: Short Trips With High Fare

# COMMAND ----------

display(spark.sql("""-- Are there suspiciously short trips with high fares after silver DQ filtering? (data quality signal)
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
ORDER BY trip_count DESC"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Q7: Top Routes by Duration

# COMMAND ----------

display(spark.sql("""-- What are the top 10 busiest origin-destination routes and their average trip duration?
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
LIMIT 10"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Q8: Fare Per Mile by Borough

# COMMAND ----------

display(spark.sql("""-- What is the average fare per mile by pickup borough?
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
ORDER BY avg_fare_per_mile DESC"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Q9: Weekend vs Weekday

# COMMAND ----------

display(spark.sql("""-- How does trip demand and revenue compare between weekdays and weekends?
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
ORDER BY avg_trips_per_day DESC"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Q10: Monthly Trend

# COMMAND ----------

display(spark.sql("""-- How do trip volume and revenue trend month over month in 2024?
SELECT
    year,
    month,
    SUM(total_trips)              AS total_trips,
    ROUND(SUM(total_revenue), 2)  AS total_revenue,
    ROUND(AVG(avg_fare), 2)       AS avg_fare,
    ROUND(AVG(avg_distance), 2)   AS avg_distance_miles
FROM gold.default.daily_metrics_by_zone
GROUP BY year, month
ORDER BY year, month"""))
