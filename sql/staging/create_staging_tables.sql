-- staging_accounts
DROP TABLE IF EXISTS staging.staging_accounts CASCADE;
CREATE TABLE staging.staging_accounts AS
SELECT
    account_id,
    account_name,
    industry,
    country,
    signup_date,
    referral_source,
    plan_tier      AS initial_plan_tier,
    seats          AS initial_seats,
    is_trial       AS initial_is_trial,
    churn_flag     AS ever_churned
FROM raw.raw_accounts;


-- staging_subscription_periods
DROP TABLE IF EXISTS staging.staging_subscription_periods CASCADE;
CREATE TABLE staging.staging_subscription_periods AS
SELECT
    subscription_id,
    account_id,
    plan_tier,
    seats,
    mrr_amount,
    arr_amount,
    start_date,
    end_date,
    billing_frequency,
    is_trial,
    upgrade_flag,
    downgrade_flag,
    churn_flag,
    CASE
        WHEN end_date IS NULL THEN TRUE
        ELSE FALSE
    END AS is_active
FROM raw.raw_subscriptions;


-- staging_months
DROP TABLE IF EXISTS staging.staging_months CASCADE;
CREATE TABLE staging.staging_months AS
WITH minmax AS (
    SELECT
        MIN(start_date) AS min_date,
        COALESCE(MAX(end_date), CURRENT_DATE) AS max_date
    FROM staging.staging_subscription_periods
),
dates AS (
    SELECT
        generate_series(
            date_trunc('month', min_date),
            date_trunc('month', max_date),
            interval '1 month'
        )::date AS month_start
    FROM minmax
)
SELECT month_start FROM dates;


-- staging_subscription_months
DROP TABLE IF EXISTS staging.staging_subscription_months CASCADE;
CREATE TABLE staging.staging_subscription_months AS
SELECT
    p.subscription_id,
    p.account_id,
    p.plan_tier,
    p.mrr_amount      AS mrr_amount,
    m.month_start     AS month,
    p.billing_frequency,
    p.is_trial,
    p.upgrade_flag,
    p.downgrade_flag,
    p.churn_flag,
    CASE
        WHEN date_trunc('month', p.start_date) = m.month_start THEN TRUE
        ELSE FALSE
    END AS is_new_mrr_month,
    CASE
        WHEN p.end_date IS NOT NULL
         AND date_trunc('month', p.end_date) = m.month_start THEN TRUE
        ELSE FALSE
    END AS is_churned_mrr_month
FROM staging.staging_subscription_periods p
JOIN staging.staging_months m
  ON m.month_start BETWEEN date_trunc('month', p.start_date)
                      AND date_trunc('month', COALESCE(p.end_date, CURRENT_DATE));