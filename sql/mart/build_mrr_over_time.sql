DROP TABLE IF EXISTS mart.mart_mrr_over_time CASCADE;

CREATE TABLE mart.mart_mrr_over_time AS
WITH acct_mrr AS (
  SELECT
    account_id,
    month,
    SUM(mrr_amount)::numeric AS mrr
  FROM staging.staging_subscription_months
  GROUP BY 1,2
),
acct_deltas AS (
  SELECT
    account_id,
    month,
    mrr,
    LAG(mrr) OVER (PARTITION BY account_id ORDER BY month) AS prev_mrr,
    mrr - LAG(mrr) OVER (PARTITION BY account_id ORDER BY month) AS delta
  FROM acct_mrr
),
exp_con AS (
  SELECT
    month,
    SUM(CASE WHEN prev_mrr > 0 AND mrr > 0 AND delta > 0 THEN delta ELSE 0 END)  AS expansion_mrr,
    SUM(CASE WHEN prev_mrr > 0 AND mrr > 0 AND delta < 0 THEN -delta ELSE 0 END) AS contraction_mrr
  FROM acct_deltas
  GROUP BY 1
),
new_churn AS (
  SELECT
    month,
    SUM(CASE WHEN is_new_mrr_month THEN mrr_amount ELSE 0 END)::numeric     AS new_mrr,
    SUM(CASE WHEN is_churned_mrr_month THEN mrr_amount ELSE 0 END)::numeric AS churned_mrr
  FROM staging.staging_subscription_months
  GROUP BY 1
),
ending AS (
  SELECT
    month,
    SUM(mrr_amount)::numeric AS ending_mrr
  FROM staging.staging_subscription_months
  GROUP BY 1
),
final AS (
  SELECT
    e.month,
    LAG(e.ending_mrr) OVER (ORDER BY e.month) AS starting_mrr,
    nc.new_mrr,
    COALESCE(ec.expansion_mrr, 0)    AS expansion_mrr,
    COALESCE(ec.contraction_mrr, 0)  AS contraction_mrr,
    nc.churned_mrr,
    e.ending_mrr
  FROM ending e
  LEFT JOIN new_churn nc ON nc.month = e.month
  LEFT JOIN exp_con ec   ON ec.month = e.month
)
SELECT
  month,
  COALESCE(starting_mrr, 0) AS starting_mrr,
  new_mrr,
  expansion_mrr,
  contraction_mrr,
  churned_mrr,
  (new_mrr + expansion_mrr - contraction_mrr - churned_mrr) AS net_new_mrr,
  ending_mrr,
  CASE
    WHEN COALESCE(starting_mrr, 0) = 0 THEN 0
    ELSE churned_mrr / NULLIF(starting_mrr, 0)
  END AS gross_revenue_churn_rate
FROM final
ORDER BY month;
