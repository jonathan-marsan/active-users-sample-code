### Tables

#### derived_user_activated
* Dataset describing when a user became active. See definition https://gist.github.com/muness/a1b0f74d264d11754d14423534abc303

#### derived_user_churned
* Dataset describing when a user churned. See definition https://gist.github.com/muness/a1b0f74d264d11754d14423534abc303

#### derived_user_became_inactive
* Dataset describing when a user became inactive, i.e. from when the "churn period" ended until eternity, unless the user becomes an active user again at a later date

#### evolving_user_status_changed
* Dataset that combines the above, describing the start and end date when a particular user is `active`, `churned` or `inactive`.

#### number
* Reference table of numbers.

#### lookup_dates
* Reference table to lookup up dates.

#### lookup_months
* Reference table to lookup up month.

#### snapshot_active_users_daily
* Dataset that shows the total (aggregate) number of active users at the beginning of each day

#### snapshot_active_users_monthly
* Dataset that shows the total (aggregate) number of active users at the beginning of the month

#### snapshot_user_churn_rate_daily
* Dataset that shows the daily churn rate

Calculation:
((total active users from previous day) - (total active users from previous day that churned on current day))/(total active users from previous day)

#### snapshot_user_churn_rate_monthly
* Dataset that shows the monthly churn rate

Calculation:
((total active users from previous month start) - (total active users from previous month that churned at current month start))/(total active users from previous month start)
