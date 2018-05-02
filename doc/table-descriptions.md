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

#### snapshot_active_users_at_month_end
* Dataset that shows the total (aggregate) number of active users at the end of the month

#### snapshot_churned_or_inactive_users_at_month_end
* Dataset that shows the total (aggregate) number of churned or inactive users at the end of the month

#### snapshot_churn_rate_by_month
* Dataset that shows the churn rate, calculated as:

((total active users at end of previous month) - (total active users at end of last month that churned or became inactive at end of current month))/(total active users at end of previous month)
