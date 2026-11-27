# Rearc Data Quest — Databricks Edition

End-to-end data ingestion, Medallion lakehouse transformation, and deployment solution built on **Databricks** (Unity Catalog Volumes, Spark Declarative Pipelines, Spark SQL, PySpark, and Databricks Asset Bundles).

## Repository Structure

```text
├── databricks.yml                                  # Databricks Asset Bundle configuration (dev & prod targets)
├── resources/
│   ├── bls_population_pipeline.yml                 # Spark Declarative Pipeline definition
│   └── ingestion_job.yml                           # Scheduled orchestration job (Step 1 -> Step 2)
├── src/
│   ├── ingestion/
│   │   └── sync_sources.py                         # Idempotent BLS & US Population ingestion into UC Volume
│   └── pipelines/
│       ├── transformations/
│       │   ├── 01_bronze.py                        # Bronze tables + header/column sanitization + expectations
│       │   ├── 02_silver.py                        # Cleaned, typed, deduplicated Silver tables + readable labels
│       │   └── 03_gold.sql                         # Primary Spark SQL Gold materialized views (Q1, Q2, Q3)
│       └── 03_gold_pyspark_alt.py                  # Documented PySpark DataFrame alternative for Gold (Q1, Q2, Q3)
├── PROCESS.md                                      # Step-by-step walkthrough, architecture, trade-offs, AI usage & retrospective
└── README.md                                       # Project overview, environment configuration, and setup instructions
