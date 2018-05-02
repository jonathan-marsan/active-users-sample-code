# Computing Active Users and Churn - Sample Code

## Objective

Create a simple ETL pipeline to compute Active Users and Churn from a sample
dataset using Amazon Redshift to model the data, Python and Luigi to run the scripts, and then Jupyter notebook to visualize the data.

#### Input Dataset Structure

| date        |  user_id  |  account_id  |  sum_tasks_used  |
| ------------|:---------:|:------------:|:----------------:|
| 2017-01-07  |  123      |  789         |  3               |

#### Output Datasets

* derived_user_activated
* derived_user_churned
* derived_user_became_inactive
* evolving_user_status_changed
* lookup_dates
* lookup_months
* number
* snapshot_active_users_daily
* snapshot_active_users_monthly
* snapshot_churn_rate_daily
* snapshot_churn_rate_monthly

Note: See `doc/table-descriptions.md` file for brief descriptions of the tables.

## Project Setup

#### Initial Setup

* If Python 3 is not installed on your system, install it: http://docs.python-guide.org/en/latest/starting/install3/osx/
* Install `virtualenv` https://virtualenv.pypa.io/en/stable/installation/
* Within your project, create a Python 3 virtual environment, e.g.  `virtualenv -p python3 venv`
* Activate the script using `source venv/bin/activate`
* Run `pip3 install -r requirements.txt` to install required packages
* When running the pipeline locally, to view the workflow visit: `http://localhost:8082/`

Note: Only tested on Mac OS

#### Environment Variables

* Add a `.env` file to the project root directory using following format,
filling out the respective values

```
export DB_USER="user_name"
export DB_PASSWORD="user_password"
export DB_HOST="redshift_host"
export DB_PORT=port_number
export DB_NAME="database_name"
export MY_SCHEMA="database_schema"
```

#### Running ETL

* Run `venv` using `source venv/bin/activate`
* To run the luigi server, type `luigid`
* In a separate terminal window, load the environment variables using `source .env`
* Type `python3 -m luigi --module pipeline end_task` to run all the entire pipeline

#### Running Jupyter Notebook
* Create a kernel to run the Jupyter notebook within the virtual environment, e.g. `python3 -m ipython kernel install --user --name=active-users`
* Within your virtual environment, start Jupyter notebook using `jupyter notebook`
* Run `active-users.ipynb` with the kernel created above
* For troubeshooting, see: https://stackoverflow.com/questions/42449814/running-jupyter-notebook-in-a-virtualenv-installed-sklearn-module-not-available
