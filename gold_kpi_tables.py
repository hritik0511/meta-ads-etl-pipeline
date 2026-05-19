# ============================================================
# GOLD LAYER - Business KPIs & Aggregated Tables
# Medallion Architecture | Meta Ads ETL Pipeline
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, round as spark_round,
    max as spark_max, min as spark_min, current_timestamp,
    countDistinct, when, lit
)

# ── Spark Session ────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("MetaAds_Gold_KPIs") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ── Read Silver Layer ────────────────────────────────────────
print("🔄 Reading Silver layer...")
df_silver = spark.read.format("delta").load("dbfs:/delta/meta_ads/silver/transformed_meta_ads")
print(f"✅ Silver records loaded: {df_silver.count()}")

# ============================================================
# GOLD TABLE 1 — Campaign Performance Summary
# ============================================================
df_campaign_summary = df_silver \
    .groupBy("campaign_id", "campaign_name", "objective") \
    .agg(
        spark_round(spark_sum("spend"), 2).alias("total_spend"),
        spark_sum("impressions").alias("total_impressions"),
        spark_sum("clicks").alias("total_clicks"),
        spark_sum("conversions").alias("total_conversions"),
        spark_round(avg("ctr"), 4).alias("avg_ctr"),
        spark_round(avg("cpc"), 2).alias("avg_cpc"),
        spark_round(avg("cpm"), 2).alias("avg_cpm"),
        spark_round(avg("conversion_rate"), 4).alias("avg_conversion_rate"),
        count("ad_id").alias("total_ads"),
        spark_sum(when(col("is_non_performing"), 1).otherwise(0)).alias("non_performing_ads")
    ) \
    .withColumn(
        "wasted_spend_estimate",
        spark_round(
            col("total_spend") * (col("non_performing_ads") / col("total_ads")), 2
        )
    ) \
    .withColumn("updated_at", current_timestamp())

print("\n📊 Campaign Performance Summary:")
df_campaign_summary.show(truncate=False)

# ============================================================
# GOLD TABLE 2 — Non-Performing Ads Detail
# ============================================================
df_non_performing = df_silver \
    .filter(col("is_non_performing") == True) \
    .select(
        "ad_id", "campaign_name", "ad_set_name", "ad_name",
        "status", "spend", "impressions", "clicks",
        "ctr", "cpc", "conversions", "date"
    ) \
    .orderBy(col("spend").desc())

print(f"\n⚠️  Non-Performing Ads (Top 10 by Spend):")
df_non_performing.show(10, truncate=False)

# ============================================================
# GOLD TABLE 3 — Daily Spend Trend
# ============================================================
df_daily_trend = df_silver \
    .groupBy("date") \
    .agg(
        spark_round(spark_sum("spend"), 2).alias("daily_spend"),
        spark_sum("impressions").alias("daily_impressions"),
        spark_sum("clicks").alias("daily_clicks"),
        spark_sum("conversions").alias("daily_conversions"),
        spark_round(avg("ctr"), 4).alias("avg_ctr")
    ) \
    .orderBy("date") \
    .withColumn("updated_at", current_timestamp())

print("\n📅 Daily Spend Trend (Last 5 Days):")
df_daily_trend.orderBy(col("date").desc()).show(5, truncate=False)

# ============================================================
# GOLD TABLE 4 — Overall KPI Summary (Single Row)
# ============================================================
df_kpi_summary = df_silver.agg(
    spark_round(spark_sum("spend"), 2).alias("total_spend"),
    spark_sum("impressions").alias("total_impressions"),
    spark_sum("clicks").alias("total_clicks"),
    spark_sum("conversions").alias("total_conversions"),
    spark_round(avg("ctr"), 4).alias("overall_ctr"),
    spark_round(avg("cpc"), 2).alias("overall_cpc"),
    spark_round(avg("cpm"), 2).alias("overall_cpm"),
    countDistinct("campaign_id").alias("total_campaigns"),
    count("ad_id").alias("total_ads"),
    spark_sum(when(col("is_non_performing"), 1).otherwise(0)).alias("total_non_performing_ads"),
    spark_round(spark_sum(
        when(col("is_non_performing"), col("spend")).otherwise(lit(0))
    ), 2).alias("total_wasted_spend")
).withColumn("updated_at", current_timestamp())

print("\n🎯 Overall KPI Summary:")
df_kpi_summary.show(truncate=False)

# ── Write all Gold Tables ────────────────────────────────────
tables = {
    "gold_campaign_summary":  ("dbfs:/delta/meta_ads/gold/campaign_summary",  df_campaign_summary),
    "gold_non_performing_ads":("dbfs:/delta/meta_ads/gold/non_performing_ads", df_non_performing),
    "gold_daily_trend":       ("dbfs:/delta/meta_ads/gold/daily_trend",        df_daily_trend),
    "gold_kpi_summary":       ("dbfs:/delta/meta_ads/gold/kpi_summary",        df_kpi_summary),
}

for table_name, (path, df) in tables.items():
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS meta_ads.{table_name}
        USING DELTA LOCATION '{path}'
    """)
    print(f"✅ {table_name} saved → {path}")

print("\n🏆 Gold Layer complete! All KPI tables ready.")
