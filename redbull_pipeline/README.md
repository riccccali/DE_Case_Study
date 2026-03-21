# Red Bull Snowflake + dbt Pipeline

This project implements the transformation layer for the Red Bull data pipeline in Snowflake using dbt.

The pipeline is organized into four layers:
- `bronze`: raw landing and quote-aware parsed ingestion tables in Snowflake
- `silver`: staging views that standardize source data
- `intermediate`: valid and rejected models that apply data quality handling
- `gold`: dimensional models for analytics and data science consumption

## End-to-end flow

```text
Cloud storage files
        |
        v
Snowpipe ingestion
        |
        +--> BRONZE *_RAW tables       (schema-drift-tolerant raw landing)
        |
        +--> BRONZE *_PARSED tables    (quote-aware structured ingestion)
                    |
                    v
             SILVER stg_* views
                    |
                    v
      INTERMEDIATE int_*_valid / int_*_rejected
                    |
                    v
          GOLD dimensions + fact table
```

## Orchestration approach

This project separates ingestion from transformation orchestration:

- Snowpipe handles file ingestion into Snowflake bronze tables.
- dbt handles transformations from bronze to silver, intermediate, and gold.

In practice, the operating model is:
1. Files arrive in cloud storage.
2. Snowpipe ingests them into bronze raw and parsed tables.
3. A scheduled transformation job runs dbt through silver, intermediate, and gold.
4. dbt tests run as part of `dbt build`.
5. Rejected rows stay visible in `int_*_rejected`.
6. Gold is the analytics-facing layer.

Two practical orchestration options for the dbt step are:

- `dbt Cloud job`
  - Pros: simplest setup for dbt runs, built-in scheduling, dbt-native observability.
  - Cons: extra platform dependency and less "all-in-Snowflake" architecture.

- `Snowflake Tasks`
  - Pros: keeps orchestration close to the warehouse and fits a Snowflake-centric design.
  - Cons: more custom setup and less convenient dbt-specific run management than dbt Cloud.

## Model structure

### Bronze

Bronze is created outside dbt in Snowflake and defined in [`snowflake_ingestion.sql`]

It contains:
- raw landing tables:
  - `MATCHING_RAW`
  - `OUTLET_RAW`
  - `PORTFOLIO_RAW`
- parsed bronze tables:
  - `MATCHING_PARSED`
  - `OUTLET_PARSED`
  - `PORTFOLIO_PARSED`

The raw tables intentionally preserve a one-string landing pattern to tolerate future schema drift. Parsed bronze tables are used as the authoritative downstream source for transformations.

### Silver

Silver staging models live in [`models/silver`]

They:
- standardize column names
- cast types
- normalize booleans and timestamps
- preserve ingestion metadata like `market`, `source_file`, and `file_row_number`
- optionally join back to raw bronze for debugging lineage through `raw_record`

### Intermediate

Intermediate models live in [`models/intermediate`]

They implement modular data quality handling:
- `int_*_valid` keeps rows that pass the current rules
- `int_*_rejected` keeps rows that fail and records:
  - `error_code`
  - `error_reason`

This is the project's current error-handling framework. It is intentionally simple now, but it is designed so that more complex rejection or remediation rules can be added later without changing downstream marts.

### Gold

Gold models live in [`models/gold`]

Current gold models:
- dimensions:
  - `dim_market`
  - `dim_platform`
  - `dim_outlet`
  - `dim_product`
- fact:
  - `fct_outlet_product_listing`

This layer is the analytics-facing dimensional model.

`fct_outlet_product_listing` contains validated outlet-product listing rows from the portfolio source, enriched with product, platform, and outlet dimension keys plus analytical attributes such as price, menu category, item position, and outlet-level Red Bull availability signals.

## Data quality approach

The project handles data quality in stages:

- `silver` enforces structural expectations:
  - required metadata exists
  - market values stay within scope
  - row identity remains stable
- `intermediate` applies rejection rules:
  - invalid rows are separated into `int_*_rejected`
  - usable rows continue in `int_*_valid`
- `gold` is built only from conformed, validated rows

Issues identified so far and current handling:

- Missing `created_at` in outlet data
  - Current handling: rejected into `int_outlets_rejected`.
  - Rationale: outlet timestamps were treated as important enough to keep the valid outlet set cleaner.

- Missing `item_price` in portfolio data
  - Current handling: rejected into `int_portfolio_rejected`.
  - Rationale: the gold fact currently depends on price being present.

- Missing `created_at` in portfolio data
  - Current handling: tolerated and kept in `int_portfolio_valid`.
  - Rationale: removing these rows would drop a large amount of otherwise useful listing data, while the current fact model does not require complete portfolio timestamps.

- Invalid or unexpected `segment_type` values in outlet data
  - Current handling: rejected into `int_outlets_rejected`.
  - Rationale: this field is exposed as a business-facing dimension attribute, so unexpected values are isolated instead of silently flowing into gold.

- Quoted tabs and delimiter issues in source files
  - Current handling: quote-aware parsed bronze tables are used as the authoritative downstream source.
  - Rationale: direct string-splitting was not robust enough for quoted tab-delimited input.

- Raw-to-parsed lineage mismatches
  - Current handling: `raw_record` joins remain warning-only in silver.
  - Rationale: bronze raw landing prioritizes schema-drift tolerance, so perfect row-level reconciliation is treated as best-effort lineage rather than a hard dependency.

- Identifier type inconsistency across tables
  - Current handling: identifier columns such as `id_outlet`, `id_platform`, and `id_ext_link` are kept as strings across the downstream models.
  - Rationale: these are identifiers, not measures, and numeric coercion caused join/runtime issues.

- Overly strict matching enrichment join logic
  - Current handling: matching enrichment in `dim_outlet` is joined at outlet level rather than outlet-platform level.
  - Rationale: the external matching feed behaves as outlet-level enrichment, and including platform in the join incorrectly defaulted some availability flags to false.

- Portfolio rows that do not map to an outlet dimension record
  - Current handling: excluded from the gold fact by requiring a non-null `outlet_key`.
  - Rationale: the gold fact is treated as a conformed analytics layer and should not contain orphaned records.

- Business-key ambiguity in product data
  - Current handling: keep a simpler `dim_product` grain for delivery speed.
  - Rationale: `id_drink` and `id_beverage` appear to represent different product levels, but the model intentionally stays simpler for this iteration.

Potential future enhancements:

- Stronger raw-to-parsed reconciliation
  - Future option: add file-level reconciliation, content fingerprints, or a dedicated lineage audit model.

- Richer rejected-row handling
  - Future option: add severity levels, allowlists, market-specific exceptions, or repair mappings instead of only valid/rejected routing.

- Product hierarchy refinement
  - Future option: split the product model into drink-level and beverage-level dimensions if size-based rollups become important.

- Historical dimensional tracking
  - Future option: add snapshots or SCD patterns if outlet or product attributes need history rather than current-state views.

- Source completeness monitoring
  - Future option: add freshness checks and file-level row-count reconciliation between raw landing and parsed bronze tables.

## Design notes

- Raw bronze intentionally prioritizes schema-drift tolerance over perfect row-level reconciliation with parsed bronze.
- Parsed bronze is the authoritative downstream source for transformations.
- `raw_record` is retained as best-effort lineage/debug metadata, not as a hard dependency for gold analytics.
- `dim_product` intentionally stays at the simpler product grain for delivery speed. If needed later, it can evolve into separate drink-level and beverage-level dimensions.
