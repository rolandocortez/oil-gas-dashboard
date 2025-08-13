{{--
  Model: int_annual_production_dedup
  Layer: MARTS (INT)
  Grain: (api_well_number, reporting_year)

  Business rules:
    - Aggregate measures by well-year:
        * SUM(oil_produced_bbl, gas_produced_mcf, water_produced_bbl) with COALESCE(0)
        * MAX(months_in_production), then cap to 12
    - Carry representative attributes from the row with the highest months_in_production
      (ties broken by higher (oil+gas+water) and non-null attributes)
    - Keep a helper field: records_aggregated
--}}

{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('stg_annual_production') }}
),

agg as (
    select
        api_well_number,
        reporting_year,
        sum(coalesce(oil_produced_bbl, 0))  as oil_produced_bbl,
        sum(coalesce(gas_produced_mcf, 0))  as gas_produced_mcf,
        sum(coalesce(water_produced_bbl, 0)) as water_produced_bbl,
        max(months_in_production)           as months_in_production_raw,
        -- cap to 12 for business logic
        least(12, max(coalesce(months_in_production, 0))) as months_in_production,
        count(*) as records_aggregated
    from base
    group by 1,2
),

ranked as (
    select
        b.*,
        -- tie-break rules:
        --   1) higher months_in_production (NULLS LAST)
        --   2) higher sum of measures
        --   3) presence of descriptive attributes (non-null first)
        row_number() over (
            partition by b.api_well_number, b.reporting_year
            order by
                b.months_in_production desc nulls last,
                (coalesce(b.oil_produced_bbl,0) + coalesce(b.gas_produced_mcf,0) + coalesce(b.water_produced_bbl,0)) desc,
                case when b.well_name              is not null then 0 else 1 end,
                case when b.company_name           is not null then 0 else 1 end,
                case when b.production_field       is not null then 0 else 1 end,
                case when b.producing_formation    is not null then 0 else 1 end
        ) as pick_rank
    from base b
),

rep as (
    select
        api_well_number,
        reporting_year,
        well_status_code,
        well_type_code,
        company_name,
        county,
        town,
        production_field,
        producing_formation,
        well_name,
        new_georeferenced_column
    from ranked
    where pick_rank = 1
)

select
    a.api_well_number,
    a.reporting_year,
    a.months_in_production,         -- capped 0..12
    a.oil_produced_bbl,
    a.gas_produced_mcf,
    a.water_produced_bbl,
    r.well_status_code,
    r.well_type_code,
    r.company_name,
    r.county,
    r.town,
    r.production_field,
    r.producing_formation,
    r.well_name,
    r.new_georeferenced_column,
    a.records_aggregated
from agg a
left join rep r
  on r.api_well_number = a.api_well_number
 and r.reporting_year  = a.reporting_year
