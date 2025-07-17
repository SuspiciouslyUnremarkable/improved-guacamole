select
  field1,
  field2,
  field3
from order_detail join order_summary on order_detail.id = order_summary.id where field1 = 5 and field2 = 6 or field3 = 7
