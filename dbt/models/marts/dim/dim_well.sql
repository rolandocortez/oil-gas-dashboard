/*
  Model: dim_well
  Layer: MARTS (DIM)
  Grain: one row per api_well_number (well)
  Source: int_annual_production_dedup (well-year aggregates)

  Business rules:
    - One record per well (api_well_number).
    - Representative attributes are picked from the row with:
        1) highest months_in_production (already capped at 12 in INT)
        2) highest sum(oil+gas+water)
        3) most recent reporting_year
    - Lifetime measures are totals across all years.
    - Join seeds to enrich status/type descriptions.
    - well_sk is a deterministic hash of api_well_number.
*/

{{ config(materialized='table') }}

with src as (
    select *
    from {{ ref('int_annual_production_dedup') }}
),

measures_per_well as (
    select
        api_well_number,
        min(reporting_year) as first_year,
        max(reporting_year) as last_year,
        sum(coalesce(oil_produced_bbl,   0)) as lifetime_oil_bbl,
        sum(coalesce(gas_produced_mcf,   0)) as lifetime_gas_mcf,
        sum(coalesce(water_produced_bbl, 0)) as lifetime_water_bbl
    from src
    group by 1
),

ranked as (
    select
        s.*,
        -- tie-break order:
        row_number() over (
            partition by s.api_well_number
            order by
                s.months_in_production desc nulls last,
                (coalesce(s.oil_produced_bbl,0) + coalesce(s.gas_produced_mcf,0) + coalesce(s.water_produced_bbl,0)) desc,
                s.reporting_year desc
        ) as pick_rank
    from src s
),

rep as (
    select
        api_well_number,
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
),

joined as (
    select
        -- deterministic surrogate key (hex string)
        lower(to_varchar(md5(cast(r.api_well_number as string)))) as well_sk,
        r.api_well_number,
        m.first_year,
        m.last_year,
        m.lifetime_oil_bbl,
        m.lifetime_gas_mcf,
        m.lifetime_water_bbl,

        -- representative attributes
        r.well_status_code,
        coalesce(sc.status_desc, 'Unknown / Not Applicable') as well_status_desc,
        r.well_type_code,
        coalesce(tc.type_desc,   'Unknown / Other')          as well_type_desc,
        r.company_name,
        r.county,
        r.town,
        r.production_field,
        r.producing_formation,
        r.well_name,
        r.new_georeferenced_column
    from rep r
    join measures_per_well m
      on m.api_well_number = r.api_well_number
    left join {{ ref('refdata_well_status_codes') }} sc
      on sc.status_code = r.well_status_code
    left join {{ ref('refdata_well_type_codes') }} tc
      on tc.type_code = r.well_type_code
)

select * from joined
