DROP TABLE IF EXISTS mart.mart_mrr_over_time CASCADE;

CREATE TABLE mart.mart_mrr_over_time AS
WITH acct_bounds AS (
    SELECT
        account_id,
        date_trunc('month', MIN(start_date))::date AS min_month,
        date_trunc('month', COALESCE(MAX(end_date), CURRENT_DATE))::date AS max_month
    FROM staging.staging_subscription_periods
    GROUP BY 1
),
acct_month_grid AS (
    SELECT
        b.account_id,
        m.month_start AS month
    FROM acct_bounds b
    JOIN staging.staging_months m
      ON m.month_start BETWEEN b.min_month AND b.max_month
),
acct_mrr AS (
    SELECT
        account_id,
        month,
        SUM(mrr_amount)::numeric AS mrr
    FROM staging.staging_subscription_months
    GROUP BY 1,2
),
acct_mrr_filled AS (
    SELECT
        g.account_id,
        g.month,
        COALESCE(a.mrr, 0)::numeric AS mrr
    FROM acct_month_grid g
    LEFT JOIN acct_mrr a
      ON a.account_id = g.account_id
     AND a.month = g.month
),
acct_deltas AS (
    SELECT
        account_id,
        month,
        mrr,
        LAG(mrr) OVER (PARTITION BY account_id ORDER BY month) AS prev_mrr,
        mrr - LAG(mrr) OVER (PARTITION BY account_id ORDER BY month) AS delta
    FROM acct_mrr_filled
),
monthly AS (
    SELECT
        month,

        -- starting MRR is last month's ending MRR
        SUM(COALESCE(prev_mrr, 0)) AS starting_mrr,

        -- new: prev was 0, now > 0
        SUM(CASE WHEN COALESCE(prev_mrr, 0) = 0 AND mrr > 0 THEN mrr ELSE 0 END) AS new_mrr,

        -- expansion: both months > 0, delta positive
        SUM(CASE WHEN COALESCE(prev_mrr, 0) > 0 AND mrr > 0 AND delta > 0 THEN delta ELSE 0 END) AS expansion_mrr,

        -- contraction: both months > 0, delta negative (store positive number)
        SUM(CASE WHEN COALESCE(prev_mrr, 0) > 0 AND mrr > 0 AND delta < 0 THEN -delta ELSE 0 END) AS contraction_mrr,

        -- churn: prev > 0, now = 0 (churned MRR equals what you lost)
        SUM(CASE WHEN COALESCE(prev_mrr, 0) > 0 AND mrr = 0 THEN prev_mrr ELSE 0 END) AS churned_mrr,

        SUM(mrr) AS ending_mrr
    FROM acct_deltas
    GROUP BY 1
)
SELECT
    month,
    starting_mrr,
    new_mrr,
    expansion_mrr,
    contraction_mrr,
    churned_mrr,
    (new_mrr + expansion_mrr - contraction_mrr - churned_mrr) AS net_new_mrr,
    ending_mrr,
    CASE
        WHEN starting_mrr = 0 THEN 0
        ELSE churned_mrr / NULLIF(starting_mrr, 0)
    END AS gross_revenue_churn_rate
FROM monthly
ORDER BY month;
