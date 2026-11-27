from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def pick_col(df, *candidates):
    for c in candidates:
        if c in df.columns:
            return F.col(c)
    return F.lit(None)


@dp.table(
    name="silver_bls_series_labels",
    comment="Cleaned BLS series catalog joined with human-readable sector, measure, duration, and seasonal labels",
)
@dp.expect_or_drop("valid_series_id", "series_id IS NOT NULL AND trim(series_id) != ''")
def silver_bls_series_labels():
    s_raw = spark.read.table("bronze_bls_series")
    s = s_raw.select(
        F.trim(pick_col(s_raw, "series_id")).alias("series_id"),
        F.trim(pick_col(s_raw, "sector_code")).alias("sector_code"),
        F.trim(pick_col(s_raw, "class_code")).alias("class_code"),
        F.trim(pick_col(s_raw, "measure_code")).alias("measure_code"),
        F.trim(pick_col(s_raw, "duration_code")).alias("duration_code"),
        F.trim(F.coalesce(pick_col(s_raw, "seasonal", "seasonal_code"), F.lit(""))).alias("seasonal_code"),
        F.trim(pick_col(s_raw, "base_year")).alias("base_year"),
        F.expr("try_cast(trim(cast(begin_year as string)) AS INT)").alias("begin_year"),
        F.trim(pick_col(s_raw, "begin_period")).alias("begin_period"),
        F.expr("try_cast(trim(cast(end_year as string)) AS INT)").alias("end_year"),
        F.trim(pick_col(s_raw, "end_period")).alias("end_period"),
    )

    sec = spark.read.table("bronze_bls_sector").select(
        F.trim("sector_code").alias("sector_code"),
        F.trim("sector_name").alias("sector_name"),
    )
    msr = spark.read.table("bronze_bls_measure").select(
        F.trim("measure_code").alias("measure_code"),
        F.trim("measure_text").alias("measure_text"),
    )
    dur = spark.read.table("bronze_bls_duration").select(
        F.trim("duration_code").alias("duration_code"),
        F.trim("duration_text").alias("duration_text"),
    )
    sea = spark.read.table("bronze_bls_seasonal").select(
        F.trim("seasonal_code").alias("seasonal_code"),
        F.trim("seasonal_text").alias("seasonal_text"),
    )

    joined = (
        s.join(sec, "sector_code", "left")
        .join(msr, "measure_code", "left")
        .join(dur, "duration_code", "left")
        .join(sea, "seasonal_code", "left")
        .dropDuplicates(["series_id"])
    )

    return joined.withColumn(
        "series_label",
        F.concat_ws(
            " | ",
            F.coalesce(F.col("sector_name"), F.col("sector_code")),
            F.coalesce(F.col("measure_text"), F.col("measure_code")),
            F.coalesce(F.col("duration_text"), F.col("duration_code")),
            F.coalesce(F.col("seasonal_text"), F.col("seasonal_code")),
        ),
    )


@dp.table(
    name="silver_bls_observations",
    comment="Cleaned, typed, and deduplicated BLS quarterly/annual observations",
)
@dp.expect_or_drop("valid_observation_keys", "series_id IS NOT NULL AND year IS NOT NULL AND period IS NOT NULL")
@dp.expect("valid_value", "value IS NOT NULL")
def silver_bls_observations():
    all_df = spark.read.table("bronze_bls_data_all").withColumn("_source_priority", F.lit(1))
    cur_df = spark.read.table("bronze_bls_data_current").withColumn("_source_priority", F.lit(2))

    unioned = all_df.unionByName(cur_df, allowMissingColumns=True).select(
        F.trim("series_id").alias("series_id"),
        F.expr("try_cast(trim(cast(year as string)) AS INT)").alias("year"),
        F.trim("period").alias("period"),
        F.expr("try_cast(trim(cast(value as string)) AS DOUBLE)").alias("value"),
        F.trim("footnote_codes").alias("footnote_codes"),
        "_ingested_from",
        "_source_priority",
    )

    w = Window.partitionBy("series_id", "year", "period").orderBy(F.col("_source_priority").asc())
    return (
        unioned.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_source_priority")
    )


@dp.table(
    name="silver_us_population",
    comment="Cleaned annual US population table",
)
@dp.expect_or_drop("valid_year", "year IS NOT NULL")
@dp.expect_or_drop("valid_population", "population IS NOT NULL AND population > 0")
def silver_us_population():
    df = spark.read.table("bronze_population")
    return (
        df.select(
            F.col("year").cast("int").alias("year"),
            F.round(F.col("population")).cast("bigint").alias("population"),
        )
        .dropDuplicates(["year"])
    )
