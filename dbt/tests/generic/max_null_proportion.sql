-- dbt/tests/generic/max_null_proportion.sql
-- Fails when NULL ratio for column_name exceeds max_prop (0..1).
{% test max_null_proportion(model, column_name, max_prop) %}
with stats as (
  select
    count(*) as total_rows,
    sum(case when {{ column_name }} is null then 1 else 0 end) as null_rows
  from {{ model }}
),
viol as (
  select
    total_rows, null_rows,
    null_rows::float / null_rows::float + (total_rows - null_rows)::float as dummy -- avoid division by zero on compile
  from stats
),
ratio as (
  select
    total_rows,
    null_rows,
    case when total_rows = 0 then 0.0 else null_rows::float / total_rows::float end as null_ratio
  from stats
)
select *
from ratio
where null_ratio > {{ max_prop }}
{% endtest %}
