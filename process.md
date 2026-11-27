# Engineering Process & Design Documentation

## 1. End-to-End Execution Walkthrough (How This Was Built & Verified)

This section documents the exact sequence followed to build, verify, and package the solution:

### Phase A: Unity Catalog Setup & Idempotent Ingestion (Step 1)
1. **Storage Provisioning:** Created a Unity Catalog schema (`workspace.rearc_raw`) and managed Volume (`/Volumes/workspace/rearc_raw/bls_population_raw`) to store raw upstream files outside DBFS root.
2. **Environment & HTTP Headers:** Configured a `requests.Session` with exponential backoff (`urllib3.util.retry.Retry`) and a compliant `User-Agent` header (`RearcDataQuest/1.0 (<contact_email>)`) plus polite request pacing (`0.2s` delay) so BLS requests succeed reliably without triggering `403 Forbidden` blocks.
3. **Idempotent BLS Directory Sync (`src/ingestion/sync_sources.py`):**
   - Parsed the HTML directory listing at `https://download.bls.gov/pub/time.series/pr/` to discover all 12 dataset/lookup files dynamically (`pr.class`, `pr.contacts`, `pr.data.0.Current`, `pr.data.1.AllData`, `pr.duration`, `pr.footnote`, `pr.measure`, `pr.period`, `pr.seasonal`, `pr.sector`, `pr.series`, `pr.txt`).
   - Maintained `/Volumes/workspace/rearc_raw/bls_population_raw/bls/_manifest.json` storing `last_modified`, `content_length`, and `sha256` per file.
   - Performed `HEAD` checks before downloading so unchanged files are skipped on re-runs, wrote updates atomically via `.part` temporary files + `os.replace`, and pruned any local files removed upstream.
4. **US Population Ingestion:** Queried DataUSA's Tesseract endpoint (`cube=acs_yg_total_population_1`, `drilldowns=Nation`, `measures=Population`, `year=all`), hashed the payload (`sha256`), and wrote `us_population.json` atomically only when content changed.
5. **Idempotency Verification:** Executed `sync_sources.py` twice in a Databricks notebook—Run 1 downloaded all 12 BLS files and `us_population.json`; Run 2 skipped all 12 BLS files (`"skipped"`) and reported `"status": "unchanged"` for population.

### Phase B: Medallion Lakehouse Pipeline (Step 2)
1. **Pipeline Configuration:** Created a serverless Lakeflow / Spark Declarative Pipeline (`rearc_bls_population_sdp`) with target catalog `workspace` and target schema `default`, organized into three transformation files under `src/pipelines/transformations/`:
   - `01_bronze.py` (Python)
   - `02_silver.py` (Python)
   - `03_gold.sql` (Spark SQL)
2. **Bronze Layer (`01_bronze.py`):**
   - Landed 8 BLS TSV tables (`bronze_bls_data_current`, `bronze_bls_data_all`, `bronze_bls_series`, `bronze_bls_sector`, `bronze_bls_measure`, `bronze_bls_duration`, `bronze_bls_seasonal`, `bronze_bls_period`) with `_ingested_from` lineage metadata and trimmed trailing spaces from TSV column headers.
   - Exploded `us_population.json` (`data` array) into `bronze_population` and sanitized invalid Delta characters (`Nation ID` -> `nation_id`).
   - Added non-dropping `@dp.expect` checks on `series_id`, `year`, and `period`.
3. **Silver Layer (`02_silver.py`):**
   - **`silver_bls_series_labels`:** Trimmed whitespace on codes and joined `pr.series` with sector, measure, duration, and seasonal lookup tables into a human-readable `series_label`.
   - **`silver_bls_observations`:** Combined `pr.data.1.AllData` (~92K rows) and `pr.data.0.Current` (~46K rows), safely cast `year` (`INT`) and `value` (`DOUBLE`) with `try_cast`, enforced `@dp.expect_or_drop` on non-null composite keys, and deduplicated `(series_id, year, period)` prioritizing `AllData`.
   - **`silver_us_population`:** Cast `year` to `INT`, rounded/cast `population` to `BIGINT`, enforced positive non-null population via `@dp.expect_or_drop`, and deduplicated on `year`.
4. **Gold Layer (`03_gold.sql` + `03_gold_pyspark_alt.py`):**
   - Implemented primary Gold materialized views in Spark SQL (`gold_q1_us_population_2013_2018`, `gold_q2_best_year_per_series`, `gold_q3_prs30006032_q01_population`) and documented identical PySpark DataFrame functions in `src/pipelines/03_gold_pyspark_alt.py`.
5. **SQL Editor Verification:** Verified all three Gold materialized views directly in Databricks SQL Editor (exact outputs recorded in Section 2 below).

### Phase C: Deployment Packaging (Step 3)
- Packaged the ingestion script and declarative pipeline into a **Databricks Asset Bundle** (`databricks.yml`, `resources/bls_population_pipeline.yml`, `resources/ingestion_job.yml`) with parameterized `catalog`, `schema`, `volume_root`, and `contact_email` variables across `dev` and `prod` targets.

---

## 2. Gold Analytical Questions & Verified Results

### Question 1: Mean and Standard Deviation of Annual US Population (2013–2018 Inclusive)
- **Gold Table:** `workspace.default.gold_q1_us_population_2013_2018`
- **Verified Output:**

| `years_included` | `mean_population` | `stddev_sample_population` | `stddev_pop_population` |
| :---: | :---: | :---: | :---: |
| `6` | `322,069,808` | `4,158,441.04` | `3,796,119.94` |

### Question 2: Best Year per BLS `series_id` (Largest Sum Across Quarters `Q01`–`Q04`) with Human-Readable Labels
- **Gold Table:** `workspace.default.gold_q2_best_year_per_series`
- **Logic:** Sums quarterly `value` across `period IN ('Q01', 'Q02', 'Q03', 'Q04')` (explicitly excluding `Q05` annual averages), ranks years per `series_id` using `ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY quarterly_total DESC, year DESC)`, and joins `silver_bls_series_labels` so each `series_id` includes `series_label`, `sector_name`, `measure_text`, and `duration_text`.
- **Sample Verified Output (`ORDER BY series_id`):**

| `series_id` | `series_label` | `sector_name` | `measure_text` | `duration_text` |
| :--- | :--- | :--- | :--- | :--- |
| `PRS30006011` | `Manufacturing \| Employment \| % Change same quarter 1 year ago \| Seasonal Adjusted` | `Manufacturing` | `Employment` | `% Change same quarter 1 year ago` |
| `PRS30006012` | `Manufacturing \| Employment \| % Change from previous quarter \| Seasonal Adjusted` | `Manufacturing` | `Employment` | `% Change from previous quarter` |
| `PRS30006013` | `Manufacturing \| Employment \| Index (2017=100) \| Seasonal Adjusted` | `Manufacturing` | `Employment` | `Index (2017=100)` |
| `PRS30006014` | `Manufacturing \| Employment \| Level \| Seasonal Adjusted` | `Manufacturing` | `Employment` | `Level` |
| `PRS30006021` | `Manufacturing \| Average weekly hours \| % Change same quarter 1 year ago \| Seasonal Adjusted` | `Manufacturing` | `Average weekly hours` | `% Change same quarter 1 year ago` |
| `PRS30006022` | `Manufacturing \| Average weekly hours \| % Change from previous quarter \| Seasonal Adjusted` | `Manufacturing` | `Average weekly hours` | `% Change from previous quarter` |
| `PRS30006023` | `Manufacturing \| Average weekly hours \| Index (2017=100) \| Seasonal Adjusted` | `Manufacturing` | `Average weekly hours` | `Index (2017=100)` |
| `PRS30006024` | `Manufacturing \| Average weekly hours \| Level \| Seasonal Adjusted` | `Manufacturing` | `Average weekly hours` | `Level` |

### Question 3: `series_id = 'PRS30006032'` and `period = 'Q01'` Left-Joined with Annual US Population
- **Gold Table:** `workspace.default.gold_q3_prs30006032_q01_population`
- **Logic:** Filters `silver_bls_observations` to `series_id = 'PRS30006032'` and `period = 'Q01'` (39 annual records) and performs a `LEFT JOIN` to `silver_us_population` on `year` so all BLS historical years are retained and `us_population` is populated where available (2013–2023) and `NULL` for earlier/unmatched years.
- **Sample Verified Output (`ORDER BY year`):**

| `series_id` | `series_label` | `year` | `period` | `series_value` | `us_population` |
| :--- | :--- | :---: | :---: | :---: | :---: |
| `PRS30006032` | `Manufacturing \| Hours worked \| % Change from previous quarter \| Seasonal Adjusted` | `1988` | `Q01` | `2.1` | `NULL` |
| `PRS30006032` | `Manufacturing \| Hours worked \| % Change from previous quarter \| Seasonal Adjusted` | `1989` | `Q01` | `1.8` | `NULL` |
| `PRS30006032` | `Manufacturing \| Hours worked \| % Change from previous quarter \| Seasonal Adjusted` | `1990` | `Q01` | `-4.6` | `NULL` |
| `PRS30006032` | `Manufacturing \| Hours worked \| % Change from previous quarter \| Seasonal Adjusted` | `1991` | `Q01` | `-7.9` | `NULL` |
| `PRS30006032` | `Manufacturing \| Hours worked \| % Change from previous quarter \| Seasonal Adjusted` | `1992` | `Q01` | `-3.1` | `NULL` |
| `PRS30006032` | `Manufacturing \| Hours worked \| % Change from previous quarter \| Seasonal Adjusted` | `1993` | `Q01` | `1.3` | `NULL` |
| `PRS30006032` | `Manufacturing \| Hours worked \| % Change from previous quarter \| Seasonal Adjusted` | `1994` | `Q01` | `1.7` | `NULL` |
| `PRS30006032` | `Manufacturing \| Hours worked \| % Change from previous quarter \| Seasonal Adjusted` | `1995` | `Q01` | `0` | `NULL` |

*(Years 2013–2023 join with annual US population records from `silver_us_population`, e.g., `2013 -> 316,128,839`, `2014 -> 318,857,056`, `2015 -> 321,418,821`.)*

---

## 3. Architecture & Design Decisions

- **Decoupled Ingestion & Transformation:** Landing raw files in a Unity Catalog Volume separates external HTTP retry/rate-limit handling from Spark compute, enabling deterministic pipeline replays without re-hitting external APIs.
- **Expectation Strategy (`@dp.expect` vs. `@dp.expect_or_drop`):** Bronze uses non-dropping expectations so raw data quality issues remain visible in pipeline metrics; Silver enforces `@dp.expect_or_drop` on primary/composite keys and positive population values to guarantee clean downstream analytics.
- **Primary Spark SQL + Documented PySpark Alternative:** Spark SQL materialized views serve as the primary Gold layer because declarative SQL is concise, readable for analysts, and natively managed by Lakeflow refresh orchestration. Equivalent PySpark DataFrame implementations (`03_gold_pyspark_alt.py`) are maintained alongside for programmatic testing and Python workflows.
- **How Safe Re-Runs & Idempotency Work:**
  - `sync_sources.py` tracks each BLS file's `Last-Modified`, `Content-Length`, and `sha256` in `_manifest.json`, skipping unchanged files via `HEAD` requests, writing updates atomically via `.part` temporary files + `os.replace`, and deleting local files removed upstream.
  - Downstream Bronze, Silver, and Gold tables recompute deterministically from the current Volume contents without duplicating rows.

---

## 4. Deployment & Client Handoff Design (Step 3)

To hand this over to a client engineering team so it runs reliably without manual notebook clicks:
- **Infrastructure-as-Code with Databricks Asset Bundles (`databricks.yml`):**
  - Parameterizes `catalog`, `schema`, `volume_root`, and `contact_email` across `dev` and `prod` targets (`resources/bls_population_pipeline.yml` and `resources/ingestion_job.yml`).
  - A single scheduled Databricks Job (`bls_population_ingestion_and_etl`) runs `sync_sources.py` first on serverless Python compute (`requests`, `urllib3`) and then triggers the Spark Declarative Pipeline (`rearc_bls_population_sdp`) upon completion.
- **CI/CD Promotion Workflow:**
  - Pull requests run `databricks bundle validate` and schema/unit checks.
  - Merges to `main` deploy to `dev` (`databricks bundle deploy -t dev`) and run an integration test; tagged releases deploy to `prod` (`databricks bundle deploy -t prod`) running under a dedicated Databricks Service Principal.
- **Note on Databricks Free Edition:** Free Edition restricts certain enterprise features (such as service principals and multi-workspace CLI/DAB promotion), so the bundle definitions are included in Git as production-ready deployment templates matching our workspace objects.

---

## 5. Trade-offs & Production Hardening

| Area | Current Implementation | Production Hardening for Scale & Operations |
| :--- | :--- | :--- |
| **Schema Drift & Upstream APIs** | Trims BLS TSV headers, normalizes JSON column names, and uses `try_cast` in Silver. | Add explicit schema/contract assertions (fail or quarantine on unexpected column removals) and alert when upstream API endpoints or payload structures change. |
| **Data Volume & Scaling** | Full batch refresh of ~138K BLS rows (`AllData` + `Current`) completes in seconds on serverless compute. | At 100x+ scale, enable Liquid Clustering on `(series_id, year)` for `silver_bls_observations`, archive historical raw snapshots by ingestion date, and switch Bronze ingestion to Auto Loader (`cloudFiles`) for incremental file discovery. |
| **Cost Optimization** | Triggered pipeline execution on serverless compute. | Align job schedule with upstream release cadence (e.g., weekly/monthly around BLS release dates rather than hourly polling) and use right-sized serverless warehouses for BI queries. |
| **Access Control & Governance** | Tables published to Unity Catalog (`workspace.default`). | Separate schemas by tier (`raw`, `bronze`, `silver`, `gold`), restrict write access to the pipeline Service Principal, and grant read-only (`USE CATALOG`, `USE SCHEMA`, `SELECT`) on `gold` tables to analyst groups. |
| **Monitoring & Alerting** | Pipeline expectation metrics tracked in Lakeflow UI. | Configure Databricks Job/Pipeline failure notifications (Slack/PagerDuty/email), add row-count anomaly checks, and monitor `_manifest.json` staleness. |

---

## 6. AI Usage

AI assistance (Gemini) was used as an engineering collaborator during development, with all code executed, inspected, and verified directly in Databricks:
- **Where AI helped:** Drafting initial boilerplate for the idempotent BLS crawler (`sync_sources.py`), structuring Spark Declarative Pipeline decorators (`@dp.table`, `@dp.expect`, `@dp.expect_or_drop`), writing the SQL/PySpark Gold queries, and scaffolding Databricks Asset Bundle YAML templates.
- **What I validated and corrected manually:**
  - Verified live HTTP responses against BLS and DataUSA endpoints, identifying that DataUSA's legacy `/api/data` endpoint returned `404` and switching to DataUSA's Tesseract endpoint.
  - Inspected pipeline run logs and SQL query outputs after each run, catching and resolving schema/casting issues in Bronze and Silver (detailed below).

---

## 7. Retrospective

### Key Bugs Encountered & How They Were Solved
1. **DataUSA Legacy API `404`:** The legacy DataUSA endpoint returned `404 Not Found`. Switching to DataUSA's Tesseract endpoint (`https://tesseract.datausa.io/tesseract/data.jsonrecords?...`) restored reliable annual US population retrieval.
2. **Trailing Whitespace in BLS TSV Headers:** BLS tab-delimited headers include padding spaces (e.g., `` `series_id          ` ``), causing `UNRESOLVED_COLUMN` errors in Bronze expectations. Adding a header-stripping loop (`col_name.strip()`) in `read_bls_tsv()` resolved this across all 8 BLS tables.
3. **Delta Lake Invalid Characters in JSON Keys:** `us_population.json` included `Nation ID`, which triggered `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES`. Sanitizing column names in `bronze_population` (`Nation ID` -> `nation_id`) made the table Delta-compliant.
4. **Scientific Notation Casting Dropping All Silver Population Rows:** `bronze_population` inferred `population` as `DOUBLE`. Casting `DOUBLE -> STRING -> BIGINT` in Silver converted values like `316128839` into scientific notation (`"3.16128839E8"`), which `try_cast(... AS BIGINT)` evaluated as `NULL` and `@dp.expect_or_drop` subsequently dropped—resulting in `years_included = 0` in Gold Q1. Casting directly via `F.round(F.col("population")).cast("bigint")` fixed the issue and produced the expected 6-year (2013–2018) statistics.

### What I Would Do Differently Next Time
- Inspect inferred schemas and sample rows (`df.printSchema()` and `LIMIT 5`) immediately after Bronze landing before writing Silver cast expressions, rather than chaining `cast(string)` on numeric columns.
- Set up a dedicated target schema (`workspace.rearc_lakehouse`) from the start to separate pipeline outputs from `workspace.default`.
