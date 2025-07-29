select count(*) as num_tests
from {{ ref('test') }}
where test_id = '{{ var("test_id", "default_test_id") }}'
  and status = 'passed'
  and created_at >= dateadd(day, -7, current_date())
  and created_at < current_date