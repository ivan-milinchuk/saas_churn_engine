DROP TABLE IF EXISTS mart.mart_mrr_over_time;
CREATE TABLE mart.mart_mrr_over_time AS
WITH base AS (
    SELECT
        month,
        SUM(mrr_amount) AS total_mrr,
        SUM(CASE WHEN is_new_mrr_month THEN mrr_amount ELSE 0 END) AS new_mrr,
        SUM(CASE WHEN is_churned_mrr_month THEN mrr_amount ELSE 0 END) AS churned_mrr
    FROM staging.staging_subscription_months
    GROUP BY month
),
ordered AS (
    SELECT
        month,
        new_mrr,
        churned_mrr,
        LAG(total_mrr) OVER (ORDER BY month) AS prev_mrr,
        total_mrr AS ending_mrr
    FROM base
)
SELECT
    month,
    COALESCE(prev_mrr, 0) AS starting_mrr,
    new_mrr,
    0::NUMERIC AS expansion_mrr,
    0::NUMERIC AS contraction_mrr,
    churned_mrr,
    (new_mrr - churned_mrr) AS net_new_mrr,
    ending_mrr,
    CASE
        WHEN COALESCE(prev_mrr, 0) = 0 THEN 0
        ELSE churned_mrr / prev_mrr
    END AS gross_revenue_churn_rate
FROM ordered
ORDER BY month;