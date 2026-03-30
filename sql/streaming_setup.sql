-- Note: In Production Enterprise environments with reservations, you would run this Continuous Query:
-- CREATE OR REPLACE CONTINUOUS QUERY {{project}}.{{dataset}}.monitor_breaches AS
-- SELECT transaction_id, cardholder_id, transaction_amount, credit_limit, CURRENT_TIMESTAMP() as breached_at
-- FROM {{project}}.{{dataset}}.simulated_transactions
-- WHERE transaction_amount > credit_limit

CREATE TABLE IF NOT EXISTS {{project}}.{{dataset}}.simulated_transactions (
    transaction_id STRING,
    cardholder_id STRING,
    transaction_amount NUMERIC,
    credit_limit NUMERIC,
    transaction_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {{project}}.{{dataset}}.breach_events (
    transaction_id STRING,
    cardholder_id STRING,
    transaction_amount NUMERIC,
    credit_limit NUMERIC,
    breached_at TIMESTAMP
);
