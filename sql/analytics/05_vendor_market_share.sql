-- How does vendor market share evolve month over month in 2024?
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
ORDER BY dd.year, dd.month, trip_count DESC
