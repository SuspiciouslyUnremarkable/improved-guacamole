-- This is test sql file for testing purposes
-- It contains various SQL statements to validate formatting and parsing
with
claim_to_employer_and_info as (select * from {{ ref('claim_to_employer_and_info') }}),
claim_weekly_summary_fact as (select * from {{ ref('claim_weekly_summary_fact') }}), 