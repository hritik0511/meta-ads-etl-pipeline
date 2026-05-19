# ============================================================
# SILVER LAYER - Data Cleaning & Transformation
# Medallion Architecture | Meta Ads ETL Pipeline
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, round as spark_round,
    current_timestamp, to_date, lit
)

# ── Spark Session ────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("MetaAds_Silver_Transform") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ── Read Bronze Layer ────────────────────────────────────────
print("🔄 Reading Bronze layer...")
df_bronze = spark.read.format("delta").load("dbfs:/delta/meta_ads/bronze/raw_meta_ads")
print(f"✅ Bronze records loaded: {df_bronze.count()}")

# ── Data Cleaning ────────────────────────────────────────────
df_clean = df_bronze \
    .dropDuplicates(["ad_id", "date"]) \
    .filter(col("spend") > 0) \
    .filter(col("impressions") > 0) \
    .na.fill({
        "clicks": 0,
        "conversions": 0,
        "video_views": 0,
        "reach": 0
    })

# ── Feature Engineering — KPI Metrics ────────────────────────
df_silver = df_clean \
    .withColumn(
        "ctr",                                                         # Click-Through Rate
        spark_round((col("clicks") / col("impressions")) * 100, 4)
    ) \
    .withColumn(
        "cpc",                                                         # Cost Per Click
        when(col("clicks") > 0,
             spark_round(col("spend") / col("clicks"), 2)
        ).otherwise(lit(None))
    ) \
    .withColumn(
        "cpm",                                                         # Cost Per 1000 Impressions
        spark_round((col("spend") / col("impressions")) * 1000, 2)
    ) \
    .withColumn(
        "conversion_rate",                                             # Conversion Rate
        when(col("clicks") > 0,
             spark_round((col("conversions") / col("clicks")) * 100, 4)
        ).otherwise(lit(0.0))
    ) \
    .withColumn(
        "cost_per_conversion",                                         # Cost Per Conversion
        when(col("conversions") > 0,
             spark_round(col("spend") / col("conversions"), 2)
        ).otherwise(lit(None))
    ) \
    .withColumn(
        "is_non_performing",                                           # Flag Non-Performing Ads
        when(
            (col("ctr") < 0.5) &                                      # CTR < 0.5%
            (col("conversions") == 0) &                               # Zero conversions
            (col("spend") > 50),                                      # Spent > $50
            lit(True)
        ).otherwise(lit(False))
    ) \
    .withColumn("transformed_at", current_timestamp()) \
    .drop("ingested_at")

# ── Show Sample ──────────────────────────────────────────────
print("\n📊 Silver Layer Sample:")
df_silver.select(
    "ad_id", "campaign_name", "spend", "impressions",
    "clicks", "ctr", "cpc", "cpm", "conversions",
    "conversion_rate", "is_non_performing"
).show(10, truncate=False)

non_performing_count = df_silver.filter(col("is_non_performing") == True).count()
print(f"\n⚠️  Non-performing ads flagged: {non_performing_count}")

# ── Write to Silver Delta Table ──────────────────────────────
silver_path = "dbfs:/delta/meta_ads/silver/transformed_meta_ads"

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_path)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS meta_ads.silver_transformed_meta_ads
    USING DELTA
    LOCATION '{silver_path}'
""")

print(f"\n✅ Silver Delta Table saved at: {silver_path}")
print("✅ Table registered: meta_ads.silver_transformed_meta_ads")
