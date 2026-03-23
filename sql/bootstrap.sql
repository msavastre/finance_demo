-- Create dataset manually if needed:
-- CREATE SCHEMA IF NOT EXISTS `your-project.rwa_demo`;

CREATE TABLE IF NOT EXISTS `{{project}}.{{dataset}}.policy_documents` (
  policy_id STRING,
  policy_version_id STRING,
  uploaded_at TIMESTAMP,
  uploaded_by STRING,
  gcs_uri STRING,
  policy_object_ref OBJECT_REF,
  status STRING,
  supersedes_policy_version_id STRING
);

CREATE TABLE IF NOT EXISTS `{{project}}.{{dataset}}.policy_extractions` (
  policy_version_id STRING,
  extraction_json JSON,
  clause_citations JSON,
  model_version STRING,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{{project}}.{{dataset}}.policy_sql_versions` (
  sql_version_id STRING,
  policy_version_id STRING,
  generated_sql STRING,
  agent_trace JSON,
  schema_snapshot JSON,
  validation_status STRING,
  approved_by STRING,
  approved_at TIMESTAMP
);

ALTER TABLE `{{project}}.{{dataset}}.policy_documents`
ADD COLUMN IF NOT EXISTS gcs_uri STRING;

ALTER TABLE `{{project}}.{{dataset}}.policy_sql_versions`
ADD COLUMN IF NOT EXISTS agent_trace JSON;

ALTER TABLE `{{project}}.{{dataset}}.policy_sql_versions`
ADD COLUMN IF NOT EXISTS schema_snapshot JSON;

CREATE TABLE IF NOT EXISTS `{{project}}.{{dataset}}.report_runs` (
  run_id STRING,
  policy_id STRING,
  policy_version_id STRING,
  sql_version_id STRING,
  run_status STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{{project}}.{{dataset}}.rwa_report_outputs` (
  portfolio STRING,
  risk_bucket STRING,
  rwa_amount NUMERIC,
  as_of_date DATE,
  run_id STRING,
  policy_id STRING,
  policy_version_id STRING,
  sql_version_id STRING
);

CREATE TABLE IF NOT EXISTS `{{project}}.{{dataset}}.exposures` (
  portfolio STRING,
  asset_class STRING,
  exposure_amount NUMERIC
);

CREATE TABLE IF NOT EXISTS `{{project}}.{{dataset}}.risk_weight_mapping` (
  asset_class STRING,
  risk_bucket STRING,
  risk_weight NUMERIC
);

INSERT INTO `{{project}}.{{dataset}}.exposures` (portfolio, asset_class, exposure_amount)
SELECT * FROM UNNEST([
  STRUCT('Treasury_Book' AS portfolio, 'Sovereign' AS asset_class, NUMERIC '20000000' AS exposure_amount),
  STRUCT('Treasury_Book', 'Bank', NUMERIC '12000000'),
  STRUCT('Markets_Book', 'Corporate', NUMERIC '15000000')
]);

INSERT INTO `{{project}}.{{dataset}}.risk_weight_mapping` (asset_class, risk_bucket, risk_weight)
SELECT * FROM UNNEST([
  STRUCT('Sovereign' AS asset_class, 'Low' AS risk_bucket, NUMERIC '0.20' AS risk_weight),
  STRUCT('Bank', 'Medium', NUMERIC '0.50'),
  STRUCT('Corporate', 'High', NUMERIC '1.00')
]);
