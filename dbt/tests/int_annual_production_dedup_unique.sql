-- Fails if there are duplicate well-year rows after dedup
select
  api_well_number,
  reporting_year
from {{ ref('int_annual_production_dedup') }}
group by 1,2
having count(*) > 1
