# ============================================================
# BRONZE LAYER - Raw Meta Ads Data Ingestion
# Medallion Architecture | Meta Ads ETL Pipeline
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, DateType
)
from pyspark.sql.functions import current_timestamp
from datetime import date, timedelta
import random

# ── Spark Session ────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("MetaAds_Bronze_Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ── Mock Data Generator ──────────────────────────────────────
campaigns = [
    "Summer_Sale_2024", "Brand_Awareness_Q1", "Retargeting_Visitors",
    "Product_Launch_X", "Holiday_Promo", "Lead_Gen_Campaign",
    "App_Install_Drive", "Video_Views_Boost"
]

ad_sets = ["AdSet_18_24", "AdSet_25_34", "AdSet_35_44", "AdSet_45_Plus"]
ad_names = ["Creative_A", "Creative_B", "Creative_C", "Video_Ad_1", "Carousel_Ad"]
objectives = ["CONVERSIONS", "REACH", "TRAFFIC", "LEAD_GENERATION", "APP_INSTALLS"]
statuses = ["ACTIVE", "ACTIVE", "ACTIVE", "PAUSED", "ACTIVE"]  # weighted active

def generate_mock_meta_ads_data(num_records=500):
    data = []
    base_date = date(2024, 1, 1)

    for i in range(num_records):
        campaign = random.choice(campaigns)
        spend = round(random.uniform(5.0, 500.0), 2)
        impressions = random.randint(500, 100000)
        clicks = random.randint(1, int(impressions * 0.1))
        conversions = random.randint(0, int(clicks * 0.2))
        reach = random.randint(int(impressions * 0.5), impressions)
        video_views = random.randint(0, impressions) if "Video" in campaign else 0
        ad_date = base_date + timedelta(days=random.randint(0, 89))  # 90-day window

        row = (
            f"ad_{i+1:04d}",                          # ad_id
            f"adset_{random.randint(1,4):03d}",        # ad_set_id
            f"camp_{campaigns.index(campaign)+1:03d}", # campaign_id
            campaign,                                   # campaign_name
            random.choice(ad_sets),                    # ad_set_name
            random.choice(ad_names),                   # ad_name
            random.choice(objectives),                 # objective
            random.choice(statuses),                   # status
            spend,                                     # spend (USD)
            impressions,                               # impressions
            clicks,                                    # clicks
            conversions,                               # conversions
            reach,                                     # reach
            video_views,                               # video_views
            ad_date                                    # date
        )
        data.append(row)
    return data

# ── Schema Definition ────────────────────────────────────────
schema = StructType([
    StructField("ad_id",          StringType(),  False),
    StructField("ad_set_id",      StringType(),  True),
    StructField("campaign_id",    StringType(),  True),
    StructField("campaign_name",  StringType(),  True),
    StructField("ad_set_name",    StringType(),  True),
    StructField("ad_name",        StringType(),  True),
    StructField("objective",      StringType(),  True),
    StructField("status",         StringType(),  True),
    StructField("spend",          DoubleType(),  True),
    StructField("impressions",    IntegerType(), True),
    StructField("clicks",         IntegerType(), True),
    StructField("conversions",    IntegerType(), True),
    StructField("reach",          IntegerType(), True),
    StructField("video_views",    IntegerType(), True),
    StructField("date",           DateType(),    True),
])

# ── Generate & Create DataFrame ──────────────────────────────
print("🔄 Generating mock Meta Ads data...")
raw_data = generate_mock_meta_ads_data(500)
df_bronze = spark.createDataFrame(raw_data, schema=schema)

# Add ingestion metadata
df_bronze = df_bronze.withColumn("ingested_at", current_timestamp())

print(f"✅ Generated {df_bronze.count()} records")
df_bronze.printSchema()
df_bronze.show(5, truncate=False)

# ── Write to Bronze Delta Table ──────────────────────────────
bronze_path = "dbfs:/delta/meta_ads/bronze/raw_meta_ads"

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(bronze_path)

# Register as table in Databricks metastore
spark.sql("CREATE DATABASE IF NOT EXISTS meta_ads")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS meta_ads.bronze_raw_meta_ads
    USING DELTA
    LOCATION '{bronze_path}'
""")

print(f"✅ Bronze Delta Table saved at: {bronze_path}")
print("✅ Table registered: meta_ads.bronze_raw_meta_ads")
