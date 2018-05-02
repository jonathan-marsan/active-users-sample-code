## Task Notes

### Tooling

#### Amazon Redshift + Python + Luigi
The queries are executed directly in Redshift because of the size of the dataset, and the potential for the dataset to grow even more in the future.

Python was only used as a wrapper to execute the queries, and to run Luigi which handles task dependencies in simple pipelines very well.

#### Jupyter Notebook
R and Rmarkdown are my typical "go to" tools when visualizing data stories outside of a tool like Looker, Domo or Mode. Since this project is relatively small in scope, it did not warrant handled package dependencies in both R and Python. Jupyter notebooks are a great alternative, which is what I used.  :)


### Exploratory Notes

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
