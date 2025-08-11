-- Model: stg_annual_production
-- Grain: row-level from RAW (duplicates may exist at well-year)
-- Keep nulls; cast to concrete types; normalize codes.

with src as (
    select * from {{ source('raw', 'ANNUAL_PRODUCTION') }}
),

casted as (
    select
        -- business keys / identifiers
        cast("api_well_number" as varchar)            as api_well_number,
        cast("api_hole_number" as varchar)            as api_hole_number,

        -- categorical / codes
        upper(trim("well_status_code"))               as well_status_code,
        upper(trim("well_type_code"))                 as well_type_code,
        trim("company_name")                          as company_name,
        trim("county")                                as county,
        trim("town")                                  as town,
        trim("production_field")                      as production_field,
        trim("producing_formation")                   as producing_formation,
        trim("well_name")                             as well_name,

        -- numerics (RAW already typed: some are FLOAT, some are NUMBER)
        cast("months_in_production" as number(38,0))  as months_in_production,
        cast("gas_produced_mcf"      as float)        as gas_produced_mcf,
        cast("water_produced_bbl"    as float)        as water_produced_bbl,
        cast("oil_produced_bbl"      as float)        as oil_produced_bbl,
        cast("reporting_year"        as number(38,0)) as reporting_year,

        -- passthrough
        trim("new_georeferenced_column")              as new_georeferenced_column
    from src
)

select * from casted
