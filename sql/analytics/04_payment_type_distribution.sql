-- How are trips distributed by payment type, and how do tips differ across types?
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
ORDER BY trip_count DESC
