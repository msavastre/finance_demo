-- Train a Real BQML Fraud Model using simulated data
CREATE OR REPLACE MODEL `{{project}}.{{dataset}}.fraud_model`
OPTIONS(
  model_type='logistic_reg',
  input_label_cols=['is_fraud_label']
) AS
SELECT 
  transaction_amount, 
  credit_limit,
  is_fraud_label
FROM `{{project}}.{{dataset}}.simulated_transactions`;
