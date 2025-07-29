select col1,  col2  as c2 ,col3   from table1


;


SELECT id, CASE WHEN amount>100 THEN 'high' ELSE 'low' END as category FROM sales

;

SELECT t1.id, t2.name, t3.value
FROM table1 t1 inner join table2 t2 on t1.id = t2.id
LEFT   join table3   t3   on t2.id=t3.fk_id


;

SELECT t.id, (SELECT count(*) FROM orders o WHERE o.user_id = t.id) as order_count
FROM users t


;

SELECT dateadd(day,7,current_date) as next_week,
round(amount,2)as rounded_amount
FROM payments


;

WITH base as (
SELECT id,name FROM users
),
orders as (
SELECT user_id, sum(total) as total FROM orders GROUP BY user_id
)
SELECT b.id, o.total FROM base b LEFT JOIN orders o ON b.id = o.user_id


;

SELECT table1.* exclude (field1, field2), field3
FROM table1


;

SELECT id, row_number() over (partition by category order by created_at desc) as rn
FROM products


;

SELECT a.id, b.value
FROM table_a a join table_b b on a.id = b.a_id
WHERE (b.status = 'active' or b.status = 'pending') and a.type = 'standard'


;

SELECT region, sum(amount) as total_amount
FROM transactions
GROUP BY region
HAVING sum(amount) > 1000


;

INSERT INTO mytable (id, name, amount)
VALUES (1,'John',100),(2,'Jane',200)


;

UPDATE accounts
SET status = CASE WHEN balance < 0 THEN 'overdue' ELSE 'ok' END
WHERE last_updated < current_date - 30


;

DELETE FROM logs WHERE created_at < current_date - 90


;

SELECT (a + (b * (c + d))) as result FROM math_table


;

-- This is a comment
SELECT 'O''Reilly' as author, "columnWithQuotes" FROM books /* block comment */
