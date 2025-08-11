-- dbt/tests/generic/non_future_year.sql
{% test non_future_year(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} > date_part(year, current_date())
{% endtest %}
