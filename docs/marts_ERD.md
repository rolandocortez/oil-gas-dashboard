# Oil & Gas Marts Design (ERD & Business Rules)

## Purpose & Scope
This document defines the dimensional model (star schema) for the analytics layer (“marts”) of the **oil-gas-dashboard** project. It covers grains, SCD approach, deduplication logic, KPIs, and testing strategy.

## Source Summary
- Primary source: `RAW.ANNUAL_PRODUCTION` (loaded from CSV via `write_pandas`).
- Known issues:
  - ~796 duplicate rows at the `(api_well_number, reporting_year)` grain.
  - Case-sensitive raw column names (quoted lowercase in Snowflake).
  - Domain irregularities in status/type codes and month ranges (handled as warnings in staging).

## Star Schema Overview
**Grain:**
- Fact table `fct_annual_production` at **well-year** grain.

**Dimensions:**
- `dim_well`: 1 row per well (API well number).
- `dim_year`: 1 row per year.

**Fact:**
- `fct_annual_production`: one row per `(well, reporting_year)` with production metrics.

### ERD (logical)
```
dim_well (well_sk) 1 ────* fct_annual_production (*well_sk, *year_sk, measures...)
dim_year (year_sk) 1 ────*
```

## Keys & Surrogate Keys
- Natural key for wells: `api_well_number`.
- Surrogate keys:
  - `well_sk`: deterministic hash of `api_well_number` (e.g., `md5`).
  - `year_sk`: `reporting_year` (int) or hash of year; simplest is `reporting_year` itself.

> We may adopt `dbt_utils.surrogate_key` later; for now we can use `md5(api_well_number)`.

## Deduplication Strategy (Business Rules)
Duplicates exist at `(api_well_number, reporting_year)`. We will **aggregate** duplicates into a single row per `(well, year)` with the following rules:

- `oil_produced_bbl`, `gas_produced_mcf`, `water_produced_bbl`: **SUM** with `NULL` treated as 0 (i.e., `coalesce` in SQL).
- `months_in_production`: **MAX** (bounded to `[0,12]` after aggregation).
- Descriptive attributes (e.g., `well_name`, `county`, `town`, `well_type_code`, `well_status_code`, `producing_formation`, `production_field`):
  - Prefer the **most frequent non-null** value across duplicates (tie-breaker: choose the value from the row with greatest `months_in_production`, then lexicographically).
  - If this is too heavy for v1, fallback: take the **non-null value from the row with max `months_in_production`**, else first non-null.

> Rationale: aggregating production metrics preserves totals; choosing representative attributes avoids inconsistent labels across duplicates.

## Dimensions

### `dim_well`
- **Grain**: 1 row per `api_well_number`.
- **Columns**:
  - `well_sk` (PK, md5 of `api_well_number`)
  - `api_well_number` (NK)
  - `well_name`
  - `company_name`
  - `county`, `town`
  - `producing_formation`, `production_field`
  - `well_type_code`, `well_type_desc` (via mapping seed)
  - `well_status_code`, `well_status_desc` (via mapping seed)
  - (optional) `first_reporting_year`, `last_reporting_year`
- **Population rule**: derive from the **deduplicated** intermediate (latest non-null values by year, or the "representative" record as defined above).
- **SCD**: Type 1 (overwrite); we do not track history by design for v1.

### `dim_year`
- **Grain**: 1 row per year present in data.
- **Columns**:
  - `year_sk` (int, same as `reporting_year`)
  - `is_current_year` (flag)
  - `is_2001_plus` (flag)
  - (optional) `decade`, etc.

## Fact

### `fct_annual_production`
- **Grain**: 1 row per `well_sk` + `year_sk`.
- **Columns**:
  - Keys: `well_sk`, `year_sk`
  - Measures:
    - `oil_produced_bbl` (float)
    - `gas_produced_mcf` (float)
    - `water_produced_bbl` (float)
    - `months_in_production` (int, 0..12)
  - Degenerate / convenience attributes:
    - `api_well_number` (degenerate for quick filters)
    - `reporting_year`
    - `county` (optional copy for quick slicing)
- **Tests**:
  - `unique` on (`well_sk`, `year_sk`)
  - `not_null` on both keys and `reporting_year`
  - `non_negative` on measures
  - `between_inclusive` 0..12 on `months_in_production`

## Code Mapping (Seeds)
Provide seed tables to map short codes to human-readable descriptions (to be joined in `dim_well`).

**`well_status_codes.csv`**
```csv
well_status_code,well_status_desc
AC,Active
IN,Inactive
NR,Never Reported
PA,Plugged & Abandoned
VP,Voided Permit
```

**`well_type_codes.csv`**
```csv
well_type_code,well_type_desc
GD,Gas Development
OD,Oil Development
NL,Not Listed / Unknown
```

> If unknown codes appear, keep the code and set description to `Unknown`.

## KPI Views

### `top_10_wells_by_oil`
- For a selected year (parameterized or latest), rank wells by `oil_produced_bbl` and return top 10 with well attributes.

### `wells_active_inactive`
- Annual counts of wells by `well_status_code`/`well_status_desc`.

### `non_productive_wells`
- Definition (v1): wells with **zero** oil and **zero** gas for the year (or `months_in_production = 0`).
- Output: list of wells per year with zero production; useful for quality checks or operational filtering.

> We avoid “non_profitable” because profitability requires price/cost context that we don’t have.

## Materialization Strategy
- `dim_*` and `fct_*`: `table`
- `int_*`: `view` for v1 (debuggability), potentially `ephemeral` in v2 for performance.
- `kpi_*`: `view`

## Performance Considerations (Snowflake)
- Consider a **cluster key** on `fct_annual_production(reporting_year)` if queries are mostly year-filtered.
- Facts are small (annual), so v1 can skip clustering safely.

## Tests & Documentation (dbt)
- Add `schema.yml` in `marts` with:
  - `unique` and `not_null` on keys, `non_negative` on measures, `between_inclusive` on months.
  - Column-level descriptions.
- Keep staging tests as warnings; enforce stricter rules in marts (e.g., `error` on negative measures).

## Implementation Plan (next steps)
1. **INT**: `int_annual_production_dedup` (group by well + year; resolve duplicates and pick representative attributes).
2. **DIM**: `dim_well` (from INT + mappings); `dim_year`.
3. **FCT**: `fct_annual_production` (join INT to dims).
4. **KPI**: `top_10_wells_by_oil`, `wells_active_inactive`, `non_productive_wells`.
5. Add `schema.yml` tests for marts and seeds for code mappings.
