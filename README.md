# Red Bull Design Document

This document covers Part 3 of the solution: architecture, scalability, data quality and observability, and operational considerations for the Red Bull Snowflake pipeline.

## 1. Architecture

### 1.1 Scope

The solution is designed for three markets in scope:
- `USA`
- `GBR`
- `DEU`

It ingests and models three related source domains:
- `outlet`: outlet and merchant metadata
- `portfolio`: outlet-product listing and menu data
- `matching`: external enrichment and Red Bull availability signals

### 1.2 End-to-end flow

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
                    |
                    v
     RBAC-controlled analytics consumption
```

### 1.3 Layer design

#### Bronze

Bronze ingestion is defined in [`snowflake_ingestion.sql`](snowflake_ingestion.sql).

For the case study, the source files are assumed to arrive in cloud object storage. An internal Snowflake stage was used to mimic that landing zone during development, and files were uploaded through Snowflake tooling before Snowpipe ingestion.

It contains:
- raw landing tables:
  - `MATCHING_RAW`
  - `OUTLET_RAW`
  - `PORTFOLIO_RAW`
- parsed bronze tables:
  - `MATCHING_PARSED`
  - `OUTLET_PARSED`
  - `PORTFOLIO_PARSED`

Design intent:
- raw landing preserves a one-string row pattern to tolerate schema drift
- parsed bronze handles quote-aware parsing and becomes the authoritative downstream source

Bronze loading alternatives considered:

- Structured raw load into many typed columns
  - Pros: immediate visibility, simpler downstream SQL, easier first inspection.
  - Cons: brittle to source column reordering/additions and more tightly coupled to the file schema.

- Single-column raw landing plus downstream parsing
  - Pros: maximum schema-drift resilience, stronger audit trail, and safer ELT-style ingestion.
  - Cons: more parsing complexity downstream and less convenient direct inspection in bronze.

The second option was chosen because it is more resilient and better aligned with a senior ELT design, even though it makes downstream parsing and lineage handling more nuanced.

#### Silver

Silver models live in [`redbull_pipeline/models/silver`](redbull_pipeline/models/silver).

They:
- standardize naming
- cast types
- normalize booleans and timestamps
- preserve ingestion metadata such as `market`, `source_file`, and `file_row_number`

#### Intermediate

Intermediate models live in [`redbull_pipeline/models/intermediate`](redbull_pipeline/models/intermediate).

They implement modular error handling:
- `int_*_valid` contains rows accepted for downstream use
- `int_*_rejected` contains rows excluded by the current rules

Rejected rows are not silently dropped. They remain queryable together with `error_code` and `error_reason`.

#### Gold

Gold models live in [`redbull_pipeline/models/gold`](redbull_pipeline/models/gold).

Current dimensional model:
- dimensions:
  - `dim_market`
  - `dim_platform`
  - `dim_outlet`
  - `dim_product`
- fact:
  - `fct_outlet_product_listing`

`fct_outlet_product_listing` contains validated outlet-product listing rows from the portfolio source, enriched with product, platform, and outlet dimension keys plus analytical attributes such as price, menu category, item position, and outlet-level availability signals.

One important gold modeling decision was in `dim_outlet`: the model intentionally keeps multiple rows for what may be the same physical shop when it appears on multiple delivery platforms. This was preferred over aggressive deduplication because platform-specific identifiers and even descriptive fields such as business name or address can vary slightly across platforms, and collapsing them too early would risk losing useful analytical context.

### 1.4 Access control

Snowflake RBAC is implemented separately in:
- [`snowflake_rbac.sql`](snowflake_rbac.sql)
- [`snowflake_rbac_demo.sql`](snowflake_rbac_demo.sql)

The access pattern is:
- market analyst roles only see their own market
- HQ role sees all markets
- row access is enforced on the gold views using a market-based row access policy

## 2. Scalability

### 2.1 Why the design scales

The architecture is intentionally layered so that each concern is isolated:

- Bronze absorbs ingestion and raw source variability.
- Silver standardizes source-specific parsing and typing.
- Intermediate separates quality handling from business presentation.
- Gold remains focused on analyst- and data-science-facing consumption.

This makes the solution easier to extend when:
- more markets are added
- more source files are added
- parsing logic changes
- quality rules become more complex

### 2.2 Schema drift strategy

The raw bronze landing tables prioritize schema-drift tolerance by keeping the landed row as a single string.

Benefits:
- the ingestion layer is less brittle when source columns evolve
- original landed content remains available for debugging and replay

Tradeoff:
- raw-to-parsed row-level reconciliation is best-effort rather than perfect

This tradeoff was accepted deliberately. The alternative, fully structured ingestion into bronze, would make downstream transformation simpler but would create tighter coupling to source-file structure and a higher likelihood of ingestion breakage when upstream files evolve.

### 2.3 Future scaling considerations

For larger production volumes, the next improvements would be:
- convert appropriate dbt models from full views to incremental tables
- introduce warehouse sizing and scheduling strategies by environment
- add more formal file-level ingestion completeness tracking
- consider splitting current-state dimensions from historical dimensions if time-based analysis becomes important

Current implementation note:
- most models are materialized as views for speed of iteration and simplicity during the case study
- this keeps the project easy to inspect, but means repeated downstream reads can scan the same underlying data more often than an incremental/table-based production design

## 3. Data Quality And Observability

### 3.1 Quality control by layer

The project handles data quality in stages:

- `silver` enforces structural expectations:
  - required metadata exists
  - market values stay within scope
  - row identity remains stable
- `intermediate` applies rejection rules:
  - invalid rows are separated into `int_*_rejected`
  - usable rows continue in `int_*_valid`
- `gold` is built only from conformed, validated rows

### 3.2 Issues identified and current handling

- Missing `created_at` in outlet data
  - Current handling: rejected into `int_outlets_rejected`
  - Rationale: outlet timestamps were treated as important enough to keep the valid outlet set cleaner

- Missing `item_price` in portfolio data
  - Current handling: rejected into `int_portfolio_rejected`
  - Rationale: the gold fact currently depends on price being present

- Missing `created_at` in portfolio data
  - Current handling: tolerated and kept in `int_portfolio_valid`
  - Rationale: removing these rows would discard a large number of otherwise useful listing rows

- Invalid or unexpected `segment_type` values in outlet data
  - Current handling: rejected into `int_outlets_rejected`
  - Rationale: this field is exposed as a business-facing descriptive attribute

- Quoted tabs and delimiter issues in source files
  - Current handling: quote-aware parsed bronze tables are used as the authoritative downstream source
  - Rationale: direct string-splitting was not robust enough for quoted tab-delimited input

- Identifier type inconsistency across tables
  - Current handling: identifier columns such as `id_outlet`, `id_platform`, and `id_ext_link` are kept as strings across downstream models
  - Rationale: these are identifiers, not measures, and numeric coercion caused join/runtime issues

- Overly strict matching enrichment join logic
  - Current handling: matching enrichment in `dim_outlet` is joined at outlet level rather than outlet-platform level
  - Rationale: the external matching feed behaves as outlet-level enrichment

- Portfolio rows that do not map to an outlet dimension record
  - Current handling: excluded from the gold fact by requiring a non-null `outlet_key`
  - Rationale: the gold fact is treated as a conformed analytics layer and should not contain orphaned records

- Business-key ambiguity in product data
  - Current handling: keep a simpler `dim_product` grain for this iteration
  - Rationale: `id_drink` and `id_beverage` appear to represent different product levels, but the simpler model is sufficient for the current scope

- Missing source-market coverage
  - Current handling: surfaced through a warning test and a persisted audit model
  - Rationale: source completeness gaps should alert the team without unnecessarily breaking the pipeline during investigation

- Stale-source freshness
  - Current handling: not enforced as a dbt freshness gate in this case study
  - Rationale: the delivered dataset is static, so freshness checks would not be meaningful here, but this would be added in production

### 3.3 Observability approach

Current observability mechanisms:
- dbt tests in silver, intermediate, and gold
- rejected-row visibility in `int_*_rejected`
- source coverage warning test:
  - [`warn_on_missing_source_coverage.sql`](redbull_pipeline/tests/warn_on_missing_source_coverage.sql)
- persisted audit view:
  - [`source_coverage_audit.sql`](redbull_pipeline/models/audit/source_coverage_audit.sql)

Recommended monitoring signals:
- expected market coverage by source
- rejected row counts over time
- row count changes between raw, parsed, valid, and gold layers
- join coverage into conformed fact tables
- data freshness by source file delivery

### 3.4 Future improvements

Potential next iterations:
- stronger raw-to-parsed reconciliation using file-level checks or lineage fingerprints
- richer rejected-row handling with severity levels, allowlists, and market-specific exception logic
- product hierarchy refinement into drink-level and beverage-level dimensions
- historical dimensional tracking through snapshots or SCD patterns
- more formal source freshness and completeness dashboards
- centralize repeated transformation and validation logic into reusable dbt macros

## 4. Operational Considerations

### 4.1 Orchestration

This project separates ingestion from transformation orchestration:

- Snowpipe handles file ingestion into Snowflake bronze tables
- dbt handles transformations from bronze to silver, intermediate, and gold

In practice:
1. files arrive in cloud storage
2. Snowpipe ingests them into bronze raw and parsed tables
3. a scheduled transformation job runs dbt through silver, intermediate, and gold
4. dbt tests run as part of `dbt build`
5. rejected rows remain visible
6. gold becomes the analytics-facing layer

Two practical orchestration options for the dbt step are:

- `dbt Cloud job`
  - Pros: simplest setup for dbt runs, built-in scheduling, dbt-native observability
  - Cons: extra platform dependency and less warehouse-native architecture

- `Snowflake Tasks`
  - Pros: keeps orchestration close to Snowflake and fits a Snowflake-centric design
  - Cons: more custom setup and less convenient dbt-specific run management than dbt Cloud

### 4.2 Reprocessing and backfills

The design supports reprocessing because:
- bronze raw landing is retained
- parsed bronze can be rebuilt when parsing logic changes
- downstream dbt layers are deterministic SQL transformations

Typical rerun strategy:
- reingest or backfill the affected bronze source
- rerun dbt from the impacted layer onward
- inspect rejected rows and coverage audits after the rerun

### 4.3 Deployment and environments

The dbt project is configured through:
- [`redbull_pipeline/dbt_project.yml`](redbull_pipeline/dbt_project.yml)
- local or job-level dbt profiles

Environment-specific concerns:
- Snowflake credentials and roles should remain outside the repo
- schema naming is controlled through the dbt schema macro in:
  - [`get_custom_schema.sql`](redbull_pipeline/macros/get_custom_schema.sql)
- source upload and landing mechanics used in the case study should be replaced by native cloud storage eventing in production

### 4.4 Known tradeoffs

- Raw bronze prioritizes schema-drift tolerance over perfect row-level reconciliation with parsed bronze
- Parsed bronze is the authoritative downstream source for transformations
- `dim_product` intentionally stays at the simpler product grain for delivery speed
- `id_drink` appears to represent a drink concept while `id_beverage` appears closer to a size-specific variant; the current model intentionally keeps the simpler single `dim_product` approach for this iteration

## 5. Repo Structure

Key files:
- [`snowflake_ingestion.sql`](snowflake_ingestion.sql)
- [`snowflake_rbac.sql`](snowflake_rbac.sql)
- [`snowflake_rbac_demo.sql`](snowflake_rbac_demo.sql)
- [`redbull_pipeline/dbt_project.yml`](redbull_pipeline/dbt_project.yml)

dbt model folders:
- [`redbull_pipeline/models/silver`](redbull_pipeline/models/silver)
- [`redbull_pipeline/models/intermediate`](redbull_pipeline/models/intermediate)
- [`redbull_pipeline/models/audit`](redbull_pipeline/models/audit)
- [`redbull_pipeline/models/gold`](redbull_pipeline/models/gold)
