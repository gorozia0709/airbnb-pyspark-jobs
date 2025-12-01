from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, current_timestamp, monotonically_increasing_id, regexp_replace, min, max, concat, broadcast

spark = SparkSession.builder \
    .appName("Build Fact Memory Optimized") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
    .config("spark.sql.autoBroadcastJoinThreshold", "50MB") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "1g") \
    .config("spark.executor.memoryOverhead", "512M") \
    .getOrCreate()

gcs_bucket = "gs://airbnb-data-bc"
output_path = f"{gcs_bucket}/dwh_parquet"
bq_dataset = "airbnb_dwh"

dim_host = spark.read.parquet(f"{output_path}/dim_host/").filter(col("is_valid") == True).cache()
dim_listing = spark.read.parquet(f"{output_path}/dim_listing/").filter(col("is_valid") == True).cache()
dim_location = spark.read.parquet(f"{output_path}/dim_location/").cache()

listings_df = spark.read.parquet(f"{gcs_bucket}/parquet/listings/")
listings_df = listings_df.withColumn("id", col("id").cast("long"))

calendar_df = spark.read.parquet(f"{gcs_bucket}/parquet/calendar/")

calendar_clean = (calendar_df
    .withColumn("listing_id", col("listing_id").cast("long"))
    .withColumn("date_id", col("date").cast("date"))
    .filter(col("date_id").isNotNull())
    .withColumn("available", 
                when(col("available") == "t", True)
                .when(col("available") == "f", False)
                .otherwise(lit(True)))
    .withColumn("price_amount", 
                when(col("price").isNull(), lit(100.00))
                .otherwise(regexp_replace(col("price"), "[$,]", "").cast("decimal(10,2)")))
    .withColumn("adjusted_price_amount", 
                when(col("adjusted_price").isNull(), col("price_amount"))
                .otherwise(regexp_replace(col("adjusted_price"), "[$,]", "").cast("decimal(10,2)")))
    .select("listing_id", "date_id", "available", "price_amount", "adjusted_price_amount")
    .repartition(100, "listing_id")
)

calendar_listing_ids = calendar_clean.select("listing_id").distinct()
dim_listing_ids = dim_listing.select("listing_src_id").distinct()

matching_listings = calendar_listing_ids.join(
    dim_listing_ids, 
    calendar_listing_ids.listing_id == dim_listing_ids.listing_src_id, 
    "inner"
)

if matching_listings.count() == 0:
    spark.stop()
    exit(1)

fact_stage1 = (calendar_clean.alias("cal")
    .join(
        broadcast(dim_listing.alias("dim_l")),
        col("cal.listing_id") == col("dim_l.listing_src_id"),
        "inner"
    )
    .select(
        col("cal.listing_id"),
        col("cal.date_id"),
        col("cal.available"),
        col("cal.price_amount"),
        col("cal.adjusted_price_amount"),
        col("dim_l.listing_id").alias("dim_listing_id"),
        col("dim_l.listing_src_id"),
        col("dim_l.minimum_nights"),
        col("dim_l.maximum_nights"),
        col("dim_l.number_of_reviews"),
        col("dim_l.number_of_reviews_ltm"),
        col("dim_l.review_scores_rating"),
        col("dim_l.availability_30")
    )
)

fact_stage2 = (fact_stage1.alias("fs1")
    .join(
        listings_df.alias("src_l").select("id", "host_id", "latitude", "longitude"),
        col("fs1.listing_src_id") == col("src_l.id"),
        "inner"
    )
    .select(
        col("fs1.*"),
        col("src_l.host_id").alias("host_src_id"),
        col("src_l.latitude"),
        col("src_l.longitude")
    )
)

fact_stage3 = (fact_stage2.alias("fs2")
    .join(
        broadcast(dim_host.alias("dim_h")),
        col("fs2.host_src_id") == col("dim_h.host_src_id"),
        "inner"
    )
    .select(
        col("fs2.*"),
        col("dim_h.host_id")
    )
)

fact_stage4 = (fact_stage3.alias("fs3")
    .join(
        broadcast(dim_location.alias("dim_loc")),
        (col("fs3.latitude").cast("decimal(10,6)") == col("dim_loc.latitude")) &
        (col("fs3.longitude").cast("decimal(10,6)") == col("dim_loc.longitude")),
        "inner"
    )
    .select(
        col("fs3.*"),
        col("dim_loc.location_id")
    )
)

fact_listing_daily = fact_stage4.select(
    monotonically_increasing_id().cast("long").alias("fact_id"),
    col("dim_listing_id").alias("listing_id"),
    col("host_id"),
    col("location_id"),
    col("date_id"),
    col("available"),
    col("price_amount"), 
    col("adjusted_price_amount"),
    col("minimum_nights"),
    col("maximum_nights"),
    col("number_of_reviews"),
    col("number_of_reviews_ltm").alias("reviews_last_year"),
    col("review_scores_rating").alias("listing_rating"),
    col("availability_30").alias("next_30_days_availability"),
    when(col("minimum_nights") <= 7, "Short-term")
     .when(col("minimum_nights") <= 30, "Medium-term")
     .otherwise("Long-term").alias("rental_category"),
    when(col("available") == True, "Available")
     .otherwise("Booked").alias("availability_status"),
    current_timestamp().alias("ta_insert_dt")
)

if fact_listing_daily.count() > 0:
    (fact_listing_daily
     .write
     .mode("overwrite")
     .option("maxRecordsPerFile", 100000)
     .parquet(f"{output_path}/fact_listing_daily/")
    )
    
    (fact_listing_daily
     .write
     .format("bigquery")
     .option("table", f"{bq_dataset}.fact_listing_daily")
     .option("temporaryGcsBucket", "airbnb-data-bc-temp")
     .option("maxParallelism", 10)
     .mode("overwrite")
     .save()
    )

dim_host.unpersist()
dim_listing.unpersist()
dim_location.unpersist()

spark.stop()