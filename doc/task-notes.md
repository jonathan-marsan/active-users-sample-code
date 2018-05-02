## Task Notes

### Tooling


#### Dataset Exploration and Tests: Data Grip
Data Grip was used to familiarize myself with the source data, test my queries, and also test output of functions that were new to me.

#### ETL: Amazon Redshift + Python + Luigi
The ETL queries to build tables were executed directly in Redshift because of the size of the dataset, and the potential for the dataset to grow even more in the future.

Python was only used as a wrapper to execute the queries, and to run Luigi which handles task dependencies in simple pipelines very well.

An example of an improvement would be to use a manifest table to reference date periods and set up the scripts to not have to drop and reload the entire tables each time the pipeline is run, but only append latest data.

#### Visualizations: Jupyter Notebook
I am more familiar with visualizations in R. Since this project is relatively small in scope, I felt package management would be a little cumbersome with two different programming languages and opted for Jupyter + Python's Seaborn package. It was a great opportunity to re-familiarize myself with Jupyter Notebooks and visualization in Python. :)

On the flip side, to actually automate these data stories or deliver them to stakeholders, more time to learn the Python Seaborn package, or a more robust tool like Looker, Domo, Mode or Plotly would be needed so x-axes are readable, numbers are visual on hover, etc. For an example of a data story and visualizations I made using R and Rmarkdown, please see: https://github.com/jonathan-marsan/nyc_restaurants


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
