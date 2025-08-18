/*
  Model: dim_year
  Layer: MARTS (DIM)
  Grain: one row per reporting year present in INT
  Source: int_annual_production_dedup (distinct reporting_year)

  Notes:
  - year_sk doubles as the business key (reporting_year).
  - Flags are handy for BI filters (current year / recent years / decade).
*/

{{ config(materialized='table') }}

with years as (
    select distinct
        cast(reporting_year as number(38,0)) as year_sk
    from {{ ref('int_annual_production_dedup') }}
    where reporting_year is not null
),

final as (
    select
        year_sk,
        year_sk                                              as year,
        /* Snowflake date functions for dynamic flags */
        case when year_sk = date_part('year', current_date()) then true else false end as is_current_year,
        case when year_sk >= date_part('year', current_date()) - 4 then true else false end as is_last_5_years,
        /* Decade helpers */
        floor(year_sk/10)*10                                 as decade_start,
        to_varchar(floor(year_sk/10)*10) || 's'              as decade_label
    from years
)

select * from final
