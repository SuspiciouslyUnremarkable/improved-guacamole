-- This is test sql file for testing purposes
-- It contains various SQL statements to validate formatting and parsing
with
claim_to_employer_and_info as (select * from {{ ref('claim_to_employer_and_info') }}),
claim_weekly_summary_fact as (select * from {{ ref('claim_weekly_summary_fact') }}), 

function_test as (SELECT cte.claim_id
        , cte.employer_id,
        cws.weekly_summary
        , dateadd('day', 7, cws.week_start_date) as next_week_start_date,case
            WHEN cws.weekly_summary is null THEN 'No Summary' ELSE 'Has Summary' END as summary_status

    FROM claim_to_employer_and_info cte

    LEFT JOIN claim_weekly_summary_fact cws
        ON cte.claim_id = cws.claim_id

)