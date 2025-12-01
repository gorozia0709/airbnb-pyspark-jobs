from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, trim, current_timestamp, year, month, dayofmonth, dayofweek, weekofyear, quarter, count, monotonically_increasing_id, broadcast, coalesce, concat, lit, row_number, explode, sequence, to_date, max as spark_max, md5
from pyspark.sql.types import DecimalType, IntegerType, DoubleType, LongType, BooleanType, TimestampType, DateType
from pyspark.sql.window import Window
from datetime import datetime
import traceback

spark = SparkSession.builder \
    .appName("Airbnb ETL - Cluster Optimized") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10MB") \
    .config("spark.executor.memory", "3g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "2") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.network.timeout", "800s") \
    .config("spark.sql.adaptive.skew.enabled", "true") \
    .getOrCreate()

gcs_bucket = "gs://airbnb-data-bc"
output_path = f"{gcs_bucket}/dwh_parquet"
bq_dataset = "airbnb_dwh"

current_ts = datetime.now()
max_date = datetime(2099, 12, 31)

listings_df = spark.read.parquet(f"{gcs_bucket}/parquet/listings/")
calendar_df = spark.read.parquet(f"{gcs_bucket}/parquet/calendar/")
reviews_df = spark.read.parquet(f"{gcs_bucket}/parquet/reviews/")

calendar_listing_col = calendar_df.schema["listing_id"].dataType

if str(calendar_listing_col) == "StringType":
    calendar_df = calendar_df.withColumn("listing_id", col("listing_id").cast(LongType()))
else:
    calendar_df = calendar_df.withColumn("listing_id", col("listing_id").cast(LongType()))

listings_df = listings_df.withColumn("id", col("id").cast(LongType()))

def replace_negatives(df, int_columns):
    for col_name in int_columns:
        df = df.withColumn(col_name, when(col(col_name) < 0, 0).otherwise(col(col_name)))
    return df

dim_host_temp = listings_df.select(
    col("host_id").cast(LongType()).alias("host_src_id"),
    col("host_name"),
    col("host_since"),
    col("host_location"),
    trim(col("host_response_time")).alias("host_response_time"),
    col("host_response_rate"),
    col("host_acceptance_rate"),
    col("host_is_superhost"),
    col("host_neighbourhood"),
    col("host_listings_count").cast(IntegerType()),
    col("host_total_listings_count").cast(IntegerType()),
    col("host_verifications"),
    col("host_has_profile_pic"),
    col("host_identity_verified"),
    col("calculated_host_listings_count").cast(IntegerType()),
    col("calculated_host_listings_count_entire_homes").cast(IntegerType()),
    col("calculated_host_listings_count_private_rooms").cast(IntegerType()),
    col("calculated_host_listings_count_shared_rooms").cast(IntegerType())
).dropDuplicates().filter(col("host_src_id").isNotNull())

int_cols = ["host_listings_count", "host_total_listings_count", 
            "calculated_host_listings_count", "calculated_host_listings_count_entire_homes",
            "calculated_host_listings_count_private_rooms", "calculated_host_listings_count_shared_rooms"]
dim_host_temp = replace_negatives(dim_host_temp, int_cols)

dim_host_temp = dim_host_temp.na.fill({
    "host_name": "N/A",
    "host_location": "N/A",
    "host_response_time": "N/A",
    "host_response_rate": "N/A",
    "host_acceptance_rate": "N/A",
    "host_is_superhost": False,
    "host_neighbourhood": "N/A",
    "host_listings_count": 0,
    "host_total_listings_count": 0,
    "host_verifications": "N/A",
    "host_has_profile_pic": False,
    "host_identity_verified": False,
    "calculated_host_listings_count": 0,
    "calculated_host_listings_count_entire_homes": 0,
    "calculated_host_listings_count_private_rooms": 0,
    "calculated_host_listings_count_shared_rooms": 0
})

try:
    existing_dim_host = spark.read.parquet(f"{output_path}/dim_host/")
    
    required_cols = ["host_id", "host_src_id", "is_valid", "start_dt", "end_dt"]
    missing_cols = [col for col in required_cols if col not in existing_dim_host.columns]
    
    if missing_cols:
        raise ValueError("Existing dimension missing SCD columns")
    
    max_host_id_result = existing_dim_host.agg(spark_max("host_id")).collect()[0]
    max_host_id = max_host_id_result[0] if max_host_id_result[0] is not None else 0
    
    current_hosts = existing_dim_host.filter(col("is_valid") == True)
    
    scd_cols = ["host_name", "host_location", "host_response_time", "host_response_rate",
                "host_acceptance_rate", "host_is_superhost", "host_neighbourhood", "host_listings_count",
                "host_total_listings_count", "host_verifications", "host_has_profile_pic",
                "host_identity_verified", "calculated_host_listings_count",
                "calculated_host_listings_count_entire_homes", "calculated_host_listings_count_private_rooms",
                "calculated_host_listings_count_shared_rooms"]
    
    dim_host_temp = dim_host_temp.withColumn(
        "row_hash", 
        md5(concat(*[coalesce(col(c).cast("string"), lit("NULL")) for c in scd_cols]))
    )
    
    current_hosts = current_hosts.withColumn(
        "row_hash",
        md5(concat(*[coalesce(col(c).cast("string"), lit("NULL")) for c in scd_cols]))
    )
    
    changed_hosts = dim_host_temp.alias("new").join(
        current_hosts.alias("curr").select("host_src_id", "host_id", "row_hash"),
        "host_src_id",
        "inner"
    ).filter(col("new.row_hash") != col("curr.row_hash"))
    
    new_hosts = dim_host_temp.alias("new").join(
        current_hosts.alias("curr").select("host_src_id"),
        "host_src_id",
        "left_anti"
    )
    
    changed_host_src_ids = changed_hosts.select("host_src_id").distinct()
    expired_hosts = existing_dim_host.alias("old").join(
        changed_host_src_ids.alias("changed"),
        col("old.host_src_id") == col("changed.host_src_id"),
        "inner"
    ).filter(col("is_valid") == True).select("old.*") \
        .withColumn("end_dt", lit(current_ts).cast(TimestampType())) \
        .withColumn("is_valid", lit(False))
    
    unchanged_hosts = existing_dim_host.alias("old").join(
        changed_host_src_ids.alias("changed"),
        col("old.host_src_id") == col("changed.host_src_id"),
        "left_anti"
    )
    
    new_versions_changed = changed_hosts.drop("row_hash") \
        .withColumn("host_id", row_number().over(Window.orderBy("host_src_id")) + max_host_id) \
        .withColumn("start_dt", lit(current_ts).cast(TimestampType())) \
        .withColumn("end_dt", lit(max_date).cast(TimestampType())) \
        .withColumn("is_valid", lit(True)) \
        .withColumn("ta_insert_dt", current_timestamp())
    
    new_host_records = new_hosts.drop("row_hash") \
        .withColumn("host_id", row_number().over(Window.orderBy("host_src_id")) + max_host_id + changed_hosts.count()) \
        .withColumn("start_dt", lit(current_ts).cast(TimestampType())) \
        .withColumn("end_dt", lit(max_date).cast(TimestampType())) \
        .withColumn("is_valid", lit(True)) \
        .withColumn("ta_insert_dt", current_timestamp())
    
    dim_host = unchanged_hosts.unionByName(expired_hosts).unionByName(new_versions_changed).unionByName(new_host_records)
    
except Exception as e:
    error_msg = str(e)
    
    if "Path does not exist" in error_msg or "No such file or directory" in error_msg or "missing SCD columns" in error_msg:
        window_spec = Window.orderBy("host_src_id")
        dim_host = dim_host_temp.withColumn("host_id", row_number().over(window_spec).cast(LongType())) \
            .withColumn("start_dt", lit(current_ts).cast(TimestampType())) \
            .withColumn("end_dt", lit(max_date).cast(TimestampType())) \
            .withColumn("is_valid", lit(True)) \
            .withColumn("ta_insert_dt", current_timestamp())
    else:
        traceback.print_exc()
        raise

dim_host = dim_host.select(
    "host_id",
    "host_name",
    "host_since",
    "host_location",
    "host_response_time",
    "host_response_rate",
    "host_acceptance_rate",
    "host_is_superhost",
    "host_neighbourhood",
    "host_listings_count",
    "host_total_listings_count",
    "host_verifications",
    "host_has_profile_pic",
    "host_identity_verified",
    "calculated_host_listings_count",
    "calculated_host_listings_count_entire_homes",
    "calculated_host_listings_count_private_rooms",
    "calculated_host_listings_count_shared_rooms",
    "start_dt",
    "end_dt",
    "is_valid",
    "ta_insert_dt",
    "host_src_id"
)

dim_host.write.mode("overwrite").parquet(f"{output_path}/dim_host/")

fresh_dim_host = spark.read.parquet(f"{output_path}/dim_host/")
fresh_dim_host.write.format("bigquery") \
    .option("table", f"{bq_dataset}.dim_host") \
    .option("temporaryGcsBucket", "airbnb-data-bc-temp") \
    .mode("overwrite") \
    .save()

dim_location_temp = listings_df.select(
    col("latitude").cast(DecimalType(10,6)),
    col("longitude").cast(DecimalType(10,6)),
    col("neighbourhood"),
    col("neighbourhood_cleansed"),
    col("neighbourhood_group_cleansed")
).filter(
    col("latitude").isNotNull() & col("longitude").isNotNull()
).withColumn(
    "location_src_id",
    concat(col("latitude").cast("string"), lit("_"), col("longitude").cast("string"))
).dropDuplicates()

dim_location_temp = dim_location_temp.na.fill({
    "neighbourhood": "N/A",
    "neighbourhood_cleansed": "N/A",
    "neighbourhood_group_cleansed": "N/A"
})

window_spec = Window.orderBy("location_src_id")
dim_location = dim_location_temp.withColumn("location_id", row_number().over(window_spec).cast(LongType())) \
    .withColumn("ta_insert_dt", current_timestamp())

dim_location = dim_location.select(
    "location_id",
    "latitude",
    "longitude",
    "neighbourhood",
    "neighbourhood_cleansed",
    "neighbourhood_group_cleansed",
    "ta_insert_dt",
    "location_src_id"
)

dim_location.write.mode("overwrite").parquet(f"{output_path}/dim_location/")
dim_location.write.format("bigquery") \
    .option("table", f"{bq_dataset}.dim_location") \
    .option("temporaryGcsBucket", "airbnb-data-bc-temp") \
    .mode("overwrite") \
    .save()

def clean_price(price_col):
    return regexp_replace(regexp_replace(price_col, "\\$", ""), ",", "").cast(DecimalType(10,2))

dim_listing_temp = listings_df.select(
    col("id").cast(LongType()).alias("listing_src_id"),
    col("name").alias("listing_name"),
    col("property_type"),
    col("room_type"),
    col("accommodates").cast(IntegerType()),
    col("bedrooms").cast(IntegerType()),
    col("beds").cast(IntegerType()),
    col("bathrooms").cast(DecimalType(10,2)),
    col("bathrooms_text"),
    col("amenities"),
    clean_price(col("price")).alias("listing_price"),
    col("minimum_nights").cast(IntegerType()),
    col("maximum_nights").cast(IntegerType()),
    col("instant_bookable"),
    col("number_of_reviews").cast(IntegerType()),
    col("number_of_reviews_ltm").cast(IntegerType()),
    col("number_of_reviews_l30d").cast(IntegerType()),
    col("first_review"),
    col("last_review"),
    col("review_scores_rating").cast(DoubleType()),
    col("review_scores_accuracy").cast(DoubleType()),
    col("review_scores_cleanliness").cast(DoubleType()),
    col("review_scores_checkin").cast(DoubleType()),
    col("review_scores_communication").cast(DoubleType()),
    col("review_scores_location").cast(DoubleType()),
    col("review_scores_value").cast(DoubleType()),
    col("reviews_per_month").cast(DecimalType(10,2)),
    col("availability_30").cast(IntegerType()),
    col("availability_60").cast(IntegerType()),
    col("availability_90").cast(IntegerType()),
    col("availability_365").cast(IntegerType()),
    col("has_availability"),
    col("license")
).dropDuplicates().filter(col("listing_src_id").isNotNull())

int_cols = ["accommodates", "bedrooms", "beds", "minimum_nights", "maximum_nights",
            "number_of_reviews", "number_of_reviews_ltm", "number_of_reviews_l30d",
            "availability_30", "availability_60", "availability_90", "availability_365"]
dim_listing_temp = replace_negatives(dim_listing_temp, int_cols)

dim_listing_temp = dim_listing_temp.na.fill({
    "listing_name": "N/A",
    "property_type": "N/A",
    "room_type": "N/A",
    "bathrooms_text": "N/A",
    "amenities": "N/A",
    "instant_bookable": False,
    "number_of_reviews": 0,
    "number_of_reviews_ltm": 0,
    "number_of_reviews_l30d": 0,
    "review_scores_rating": 0.0,
    "review_scores_accuracy": 0.0,
    "review_scores_cleanliness": 0.0,
    "review_scores_checkin": 0.0,
    "review_scores_communication": 0.0,
    "review_scores_location": 0.0,
    "review_scores_value": 0.0,
    "reviews_per_month": 0.0,
    "availability_30": 0,
    "availability_60": 0,
    "availability_90": 0,
    "availability_365": 0,
    "has_availability": False,
    "license": "N/A"
})

try:
    existing_dim_listing = spark.read.parquet(f"{output_path}/dim_listing/")
    
    max_listing_id_result = existing_dim_listing.agg(spark_max("listing_id")).collect()[0]
    max_listing_id = max_listing_id_result[0] if max_listing_id_result[0] is not None else 0
    
    current_listings = existing_dim_listing.filter(col("is_valid") == True)
    
    scd_cols = ["listing_name", "property_type", "room_type", "accommodates", "bedrooms", "beds",
                "bathrooms", "bathrooms_text", "amenities", "listing_price", "minimum_nights",
                "maximum_nights", "instant_bookable", "number_of_reviews", "number_of_reviews_ltm",
                "number_of_reviews_l30d", "first_review", "last_review", "review_scores_rating",
                "review_scores_accuracy", "review_scores_cleanliness", "review_scores_checkin",
                "review_scores_communication", "review_scores_location", "review_scores_value",
                "reviews_per_month", "availability_30", "availability_60", "availability_90",
                "availability_365", "has_availability", "license"]
    
    dim_listing_temp = dim_listing_temp.withColumn(
        "row_hash", 
        md5(concat(*[coalesce(col(c).cast("string"), lit("NULL")) for c in scd_cols]))
    )
    
    current_listings = current_listings.withColumn(
        "row_hash",
        md5(concat(*[coalesce(col(c).cast("string"), lit("NULL")) for c in scd_cols]))
    )
    
    changed_listings = dim_listing_temp.alias("new").join(
        current_listings.alias("curr").select("listing_src_id", "listing_id", "row_hash"),
        "listing_src_id",
        "inner"
    ).filter(col("new.row_hash") != col("curr.row_hash"))
    
    new_listings = dim_listing_temp.alias("new").join(
        current_listings.alias("curr").select("listing_src_id"),
        "listing_src_id",
        "left_anti"
    )
    
    changed_listing_src_ids = changed_listings.select("listing_src_id").distinct()
    expired_listings = existing_dim_listing.alias("old").join(
        changed_listing_src_ids.alias("changed"),
        col("old.listing_src_id") == col("changed.listing_src_id"),
        "inner"
    ).filter(col("is_valid") == True).select("old.*") \
        .withColumn("end_dt", lit(current_ts).cast(TimestampType())) \
        .withColumn("is_valid", lit(False))
    
    unchanged_listings = existing_dim_listing.alias("old").join(
        changed_listing_src_ids.alias("changed"),
        col("old.listing_src_id") == col("changed.listing_src_id"),
        "left_anti"
    )
    
    new_versions_changed = changed_listings.drop("row_hash") \
        .withColumn("listing_id", row_number().over(Window.orderBy("listing_src_id")) + max_listing_id) \
        .withColumn("start_dt", lit(current_ts).cast(TimestampType())) \
        .withColumn("end_dt", lit(max_date).cast(TimestampType())) \
        .withColumn("is_valid", lit(True)) \
        .withColumn("ta_insert_dt", current_timestamp())
    
    new_listing_records = new_listings.drop("row_hash") \
        .withColumn("listing_id", row_number().over(Window.orderBy("listing_src_id")) + max_listing_id + changed_listings.count()) \
        .withColumn("start_dt", lit(current_ts).cast(TimestampType())) \
        .withColumn("end_dt", lit(max_date).cast(TimestampType())) \
        .withColumn("is_valid", lit(True)) \
        .withColumn("ta_insert_dt", current_timestamp())
    
    dim_listing = unchanged_listings.unionByName(expired_listings).unionByName(new_versions_changed).unionByName(new_listing_records)
    
except Exception as e:
    error_msg = str(e)
    
    if "Path does not exist" in error_msg or "No such file or directory" in error_msg or "missing SCD columns" in error_msg:
        window_spec = Window.orderBy("listing_src_id")
        dim_listing = dim_listing_temp.withColumn("listing_id", row_number().over(window_spec).cast(LongType())) \
            .withColumn("start_dt", lit(current_ts).cast(TimestampType())) \
            .withColumn("end_dt", lit(max_date).cast(TimestampType())) \
            .withColumn("is_valid", lit(True)) \
            .withColumn("ta_insert_dt", current_timestamp())
    else:
        traceback.print_exc()
        raise

dim_listing = dim_listing.select(
    "listing_id",
    "listing_name",
    "property_type",
    "room_type",
    "accommodates",
    "bedrooms",
    "beds",
    "bathrooms",
    "bathrooms_text",
    "amenities",
    "listing_price",
    "minimum_nights",
    "maximum_nights",
    "instant_bookable",
    "number_of_reviews",
    "number_of_reviews_ltm",
    "number_of_reviews_l30d",
    "first_review",
    "last_review",
    "review_scores_rating",
    "review_scores_accuracy",
    "review_scores_cleanliness",
    "review_scores_checkin",
    "review_scores_communication",
    "review_scores_location",
    "review_scores_value",
    "reviews_per_month",
    "availability_30",
    "availability_60",
    "availability_90",
    "availability_365",
    "has_availability",
    "license",
    "start_dt",
    "end_dt",
    "is_valid",
    "ta_insert_dt",
    "listing_src_id"
)

dim_listing.write.mode("overwrite").parquet(f"{output_path}/dim_listing/")

spark.catalog.clearCache()
fresh_dim_listing = spark.read.parquet(f"{output_path}/dim_listing/")

fresh_dim_listing.write.format("bigquery") \
    .option("table", f"{bq_dataset}.dim_listing") \
    .option("temporaryGcsBucket", "airbnb-data-bc-temp") \
    .mode("overwrite") \
    .save()

dim_date = spark.sql("""
    SELECT explode(sequence(to_date('2010-01-01'), to_date('2030-12-31'), interval 1 day)) as date_id
""")

dim_date = dim_date.select(
    "date_id",
    year("date_id").alias("year"),
    quarter("date_id").alias("quarter"),
    month("date_id").alias("month"),
    weekofyear("date_id").alias("week_of_year"),
    dayofmonth("date_id").alias("day_of_month"),
    dayofweek("date_id").alias("day_of_week"),
    when(dayofweek("date_id")==1,"Sunday")
    .when(dayofweek("date_id")==2,"Monday")
    .when(dayofweek("date_id")==3,"Tuesday")
    .when(dayofweek("date_id")==4,"Wednesday")
    .when(dayofweek("date_id")==5,"Thursday")
    .when(dayofweek("date_id")==6,"Friday")
    .when(dayofweek("date_id")==7,"Saturday")
    .alias("day_name"),
    when(dayofweek("date_id").isin([1,7]), True).otherwise(False).alias("is_weekend")
)

dim_date.write.mode("overwrite").parquet(f"{output_path}/dim_date/")
dim_date.write.format("bigquery") \
    .option("table", f"{bq_dataset}.dim_date") \
    .option("temporaryGcsBucket", "airbnb-data-bc-temp") \
    .mode("overwrite") \
    .save()

spark.stop()