SELECT
    requires_table_reference.field1
    , requires_table_reference.field2
    , requires_table_reference.field3
FROM order_detail
INNER JOIN order_summary
    ON order_detail.id = order_summary.id
WHERE requires_table_reference.field1 = 5
    AND requires_table_reference.field2 = 6
    OR requires_table_reference.field3 = 7
