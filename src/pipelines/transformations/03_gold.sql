-- Q1: Mean and standard deviation of annual US population across 2013-2018 inclusive
CREATE OR REFRESH MATERIALIZED VIEW gold_q1_us_population_2013_2018
COMMENT 'Mean and standard deviation of US population across 2013-2018 inclusive'
AS
SELECT
  COUNT(*) AS years_included,
  ROUND(AVG(population), 2) AS mean_population,
  ROUND(STDDEV_SAMP(population), 2) AS stddev_sample_population,
  ROUND(STDDEV_POP(population), 2) AS stddev_pop_population
FROM silver_us_population
WHERE year BETWEEN 2013 AND 2018;


-- Q2: For every series_id, best year (largest sum across quarters Q01-Q04) + human-readable label
CREATE OR REFRESH MATERIALIZED VIEW gold_q2_best_year_per_series
COMMENT 'Best year per BLS series based on sum of quarterly values (Q01-Q04), enriched with human-readable labels'
AS
WITH quarterly_sums AS (
  SELECT
    o.series_id,
    o.year,
    SUM(o.value) AS quarterly_total
  FROM silver_bls_observations o
  WHERE o.period IN ('Q01', 'Q02', 'Q03', 'Q04')
    AND o.value IS NOT NULL
  GROUP BY o.series_id, o.year
),
ranked_years AS (
  SELECT
    series_id,
    year AS best_year,
    quarterly_total AS best_year_quarterly_sum,
    ROW_NUMBER() OVER (
      PARTITION BY series_id
      ORDER BY quarterly_total DESC, year DESC
    ) AS rn
  FROM quarterly_sums
)
SELECT
  r.series_id,
  COALESCE(lbl.series_label, r.series_id) AS series_label,
  lbl.sector_name,
  lbl.measure_text,
  lbl.duration_text,
  r.best_year,
  ROUND(r.best_year_quarterly_sum, 4) AS best_year_quarterly_sum
FROM ranked_years r
LEFT JOIN silver_bls_series_labels lbl
  ON r.series_id = lbl.series_id
WHERE r.rn = 1;


-- Q3: series_id = PRS30006032 and period = Q01 each year, joined with that year's population where available
CREATE OR REFRESH MATERIALIZED VIEW gold_q3_prs30006032_q01_population
COMMENT 'Annual Q01 values for series PRS30006032 left-joined with US population where available'
AS
SELECT
  o.series_id,
  COALESCE(lbl.series_label, o.series_id) AS series_label,
  o.year,
  o.period,
  o.value AS series_value,
  p.population AS us_population
FROM silver_bls_observations o
LEFT JOIN silver_bls_series_labels lbl
  ON o.series_id = lbl.series_id
LEFT JOIN silver_us_population p
  ON o.year = p.year
WHERE o.series_id = 'PRS30006032'
  AND o.period = 'Q01'
ORDER BY o.year;
