from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, regexp_replace
from pyspark.sql.types import *

# ---------------------------
# Spark session
# ---------------------------
spark = SparkSession.builder \
    .appName("Airbnb CSV to Parquet") \
    .getOrCreate()

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# ---------------------------
# Input / Output paths
# ---------------------------
listings_csv_path = "gs://airbnb-data-bc/csv/listings/"
reviews_csv_path = "gs://airbnb-data-bc/csv/reviews/"
calendar_csv_path = "gs://airbnb-data-bc/csv/calendar/"

listings_parquet_path = "gs://airbnb-data-bc/parquet/listings/"
reviews_parquet_path = "gs://airbnb-data-bc/parquet/reviews/"
calendar_parquet_path = "gs://airbnb-data-bc/parquet/calendar/"

# ============================================================
# 1. LISTINGS
# ============================================================
listings_df = spark.read \
    .option("header", True) \
    .option("multiLine", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(listings_csv_path)

listings_df_parquet = listings_df \
    .withColumn("id", col("id").cast(StringType())) \
    .withColumn("host_id", col("host_id").cast(StringType())) \
    .withColumn("scrape_id", col("scrape_id").cast(StringType())) \
    .withColumn("last_scraped", col("last_scraped").cast(TimestampType())) \
    .withColumn("host_since", col("host_since").cast(DateType())) \
    .withColumn("host_is_superhost", when(lower(col("host_is_superhost")).isin("t", "true"), True).otherwise(False)) \
    .withColumn("host_has_profile_pic", when(lower(col("host_has_profile_pic")).isin("t", "true"), True).otherwise(False)) \
    .withColumn("host_identity_verified", when(lower(col("host_identity_verified")).isin("t", "true"), True).otherwise(False)) \
    .withColumn("latitude", col("latitude").cast(DecimalType(10,6))) \
    .withColumn("longitude", col("longitude").cast(DecimalType(10,6))) \
    .withColumn("accommodates", col("accommodates").cast(IntegerType())) \
    .withColumn("bathrooms", col("bathrooms").cast(DecimalType(10,2))) \
    .withColumn("bedrooms", col("bedrooms").cast(IntegerType())) \
    .withColumn("beds", col("beds").cast(IntegerType())) \
    .withColumn("price", regexp_replace(col("price"), "[$,]", "").cast(DecimalType(10,2))) \
    .withColumn("minimum_nights", col("minimum_nights").cast(IntegerType())) \
    .withColumn("maximum_nights", col("maximum_nights").cast(IntegerType())) \
    .withColumn("minimum_minimum_nights", col("minimum_minimum_nights").cast(IntegerType())) \
    .withColumn("maximum_minimum_nights", col("maximum_minimum_nights").cast(IntegerType())) \
    .withColumn("minimum_maximum_nights", col("minimum_maximum_nights").cast(IntegerType())) \
    .withColumn("maximum_maximum_nights", col("maximum_maximum_nights").cast(IntegerType())) \
    .withColumn("minimum_nights_avg_ntm", col("minimum_nights_avg_ntm").cast(DecimalType(10,2))) \
    .withColumn("maximum_nights_avg_ntm", col("maximum_nights_avg_ntm").cast(DecimalType(10,2))) \
    .withColumn("calendar_updated", col("calendar_updated").cast(DateType())) \
    .withColumn("has_availability", when(lower(col("has_availability")).isin("t", "true"), True).otherwise(False)) \
    .withColumn("availability_30", col("availability_30").cast(IntegerType())) \
    .withColumn("availability_60", col("availability_60").cast(IntegerType())) \
    .withColumn("availability_90", col("availability_90").cast(IntegerType())) \
    .withColumn("availability_365", col("availability_365").cast(IntegerType())) \
    .withColumn("calendar_last_scraped", col("calendar_last_scraped").cast(DateType())) \
    .withColumn("number_of_reviews", col("number_of_reviews").cast(IntegerType())) \
    .withColumn("number_of_reviews_ltm", col("number_of_reviews_ltm").cast(IntegerType())) \
    .withColumn("number_of_reviews_l30d", col("number_of_reviews_l30d").cast(IntegerType())) \
    .withColumn("first_review", col("first_review").cast(DateType())) \
    .withColumn("last_review", col("last_review").cast(DateType())) \
    .withColumn("review_scores_rating", col("review_scores_rating").cast(DoubleType())) \
    .withColumn("review_scores_accuracy", col("review_scores_accuracy").cast(DoubleType())) \
    .withColumn("review_scores_cleanliness", col("review_scores_cleanliness").cast(DoubleType())) \
    .withColumn("review_scores_checkin", col("review_scores_checkin").cast(DoubleType())) \
    .withColumn("review_scores_communication", col("review_scores_communication").cast(DoubleType())) \
    .withColumn("review_scores_location", col("review_scores_location").cast(DoubleType())) \
    .withColumn("review_scores_value", col("review_scores_value").cast(DoubleType())) \
    .withColumn("instant_bookable", when(lower(col("instant_bookable")).isin("t", "true"), True).otherwise(False)) \
    .withColumn("calculated_host_listings_count", col("calculated_host_listings_count").cast(IntegerType())) \
    .withColumn("calculated_host_listings_count_entire_homes", col("calculated_host_listings_count_entire_homes").cast(IntegerType())) \
    .withColumn("calculated_host_listings_count_private_rooms", col("calculated_host_listings_count_private_rooms").cast(IntegerType())) \
    .withColumn("calculated_host_listings_count_shared_rooms", col("calculated_host_listings_count_shared_rooms").cast(IntegerType())) \
    .withColumn("reviews_per_month", col("reviews_per_month").cast(DecimalType(10,2)))

listings_df_parquet.write.mode("overwrite").parquet(listings_parquet_path)

# ============================================================
# 2. CALENDAR
# ============================================================
calendar_df = spark.read \
    .option("header", True) \
    .option("multiLine", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(calendar_csv_path)

calendar_df_parquet = calendar_df \
    .withColumn("listing_id", col("listing_id").cast(StringType())) \
    .withColumn("date", col("date").cast(DateType())) \
    .withColumn("available", when(lower(col("available")).isin("t", "true"), True).otherwise(False)) \
    .withColumn("price", regexp_replace(col("price"), "[$,]", "").cast(DecimalType(10,2))) \
    .withColumn("adjusted_price", regexp_replace(col("adjusted_price"), "[$,]", "").cast(DecimalType(10,2))) \
    .withColumn("minimum_nights", col("minimum_nights").cast(IntegerType())) \
    .withColumn("maximum_nights", col("maximum_nights").cast(IntegerType()))

calendar_df_parquet.write.mode("overwrite").parquet(calendar_parquet_path)

# ============================================================
# 3. REVIEWS
# ============================================================
reviews_df = spark.read \
    .option("header", True) \
    .option("multiLine", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(reviews_csv_path)

reviews_df_parquet = reviews_df \
    .withColumn("listing_id", col("listing_id").cast(StringType())) \
    .withColumn("id", col("id").cast(StringType())) \
    .withColumn("reviewer_id", col("reviewer_id").cast(StringType())) \
    .withColumn("date", col("date").cast(DateType()))

reviews_df_parquet.write.mode("overwrite").parquet(reviews_parquet_path)

# ============================================================
# Done
# ============================================================
spark.stop()
