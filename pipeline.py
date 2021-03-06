"""
ETL pipeline
"""

import luigi
from luigi.contrib.simulate import RunAnywayTarget

from utilities.db_connection import db_connection

from tables.compute_load_user_tasks_with_active_period import load_user_tasks_with_active_period
from tables.compute_load_derived_user_activated import load_derived_user_activated
from tables.compute_load_derived_user_became_inactive import load_derived_user_became_inactive
from tables.compute_load_derived_user_churned import load_derived_user_churned
from tables.compute_load_evolving_user_status_changed import load_evolving_user_status_changed
from tables.compute_load_number import load_number
from tables.compute_load_lookup_dates import load_lookup_dates
from tables.compute_load_lookup_months import load_lookup_months
from tables.compute_load_snapshot_active_users_daily import load_snapshot_active_users_daily
from tables.compute_load_snapshot_active_users_monthly import load_snapshot_active_users_monthly
from tables.compute_load_snapshot_user_churn_rate_daily import load_snapshot_user_churn_rate_daily
from tables.compute_load_snapshot_user_churn_rate_monthly import load_snapshot_user_churn_rate_monthly


class number(luigi.Task):
    def requires(self):
        return None
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        load_number(db_connection())
        return self.output().done()


class lookup_dates(luigi.Task):
    def requires(self):
        return [number()]
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        load_lookup_dates(db_connection())
        return self.output().done()


class lookup_months(luigi.Task):
    def requires(self):
        return [lookup_dates()]
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        load_lookup_months(db_connection())
        return self.output().done()


class user_tasks_with_active_period(luigi.Task):
    def requires(self):
        return None
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        load_user_tasks_with_active_period(db_connection())
        return self.output().done()


class derived_user_activated(luigi.Task):
    def requires(self):
        return [user_tasks_with_active_period()]
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        load_derived_user_activated(db_connection())
        return self.output().done()


class derived_user_became_inactive(luigi.Task):
    def requires(self):
        return [user_tasks_with_active_period()]
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        load_derived_user_became_inactive(db_connection())
        return self.output().done()


class derived_user_churned(luigi.Task):
    def requires(self):
        return [user_tasks_with_active_period()]
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        load_derived_user_churned(db_connection())
        return self.output().done()


class evolving_user_status_changed(luigi.Task):
    def requires(self):
        return [derived_user_activated(), derived_user_became_inactive(),
                derived_user_churned()]
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        load_evolving_user_status_changed(db_connection())
        return self.output().done()


class snapshot_active_users_daily(luigi.Task):
    def requires(self):
        return [evolving_user_status_changed(),
                lookup_dates()]
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        load_snapshot_active_users_daily(db_connection())
        return self.output().done()


class snapshot_active_users_monthly(luigi.Task):
    def requires(self):
        return [evolving_user_status_changed(),
                lookup_months()]
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        load_snapshot_active_users_monthly(db_connection())
        return self.output().done()


class snapshot_user_churn_rate_daily(luigi.Task):
    def requires(self):
        return [evolving_user_status_changed(),
                lookup_dates()]
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        load_snapshot_user_churn_rate_daily(db_connection())
        return self.output().done()


class snapshot_user_churn_rate_monthly(luigi.Task):
    def requires(self):
        return [evolving_user_status_changed(),
                lookup_months()]
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        load_snapshot_user_churn_rate_monthly(db_connection())
        return self.output().done()


class end_task(luigi.Task):
    def requires(self):
        return [snapshot_active_users_daily(),
                snapshot_active_users_monthly(),
                snapshot_user_churn_rate_daily(),
                snapshot_user_churn_rate_monthly()]
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        print("Pipeline successfully run. Ending run.")
        return self.output().done()


if __name__ == '__main__':
    luigi.run()
