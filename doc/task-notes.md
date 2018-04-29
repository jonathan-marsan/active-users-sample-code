## Task Notes

#### Q1: Is a row added when users do not perform any task in a given day?

```
SELECT
  *
FROM
  source_data.tasks_used_da
WHERE
  sum_tasks_used = 0
LIMIT
  10
```
**Answer** Yes

#### Q2 Can a single user be associated to several accounts?

```
SELECT
  user_id,
  COUNT(DISTINCT account_id)
FROM
  source_data.tasks_used_da
GROUP BY
  user_id
ORDER BY
  COUNT(DISTINCT account_id) DESC
LIMIT
  10
```
**Answer** Yes
