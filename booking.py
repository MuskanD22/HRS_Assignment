from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, date_add, col, when
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName("BookingsETL").getOrCreate()

# Load the dataset into a Spark DataFrame
bookings = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("C:/Users/mohit/Desktop/HRS/Question2_Input.csv")

# Filter the DataFrame to only include bookings where the Market Segment is designated as Tour Operators
filtered_bookings = bookings.filter(col("market_segment") == "Tour Operators")

filtered_bookings = filtered_bookings.withColumn("arrival_date", concat_ws("-", col("arrival_date_year"), col("arrival_date_month"), col("arrival_date_day_of_month"))) \
                                     .withColumn("departure_date", expr("date_add(arrival_date, CAST(stays_in_weekend_nights AS INT) + CAST(stays_in_week_nights AS INT))"))

# Create a new column in the filtered DataFrame, with_family_breakfast
filtered_bookings = filtered_bookings.withColumn("with_family_breakfast", when(col("children") + col("babies") > 0, "Yes").otherwise("No"))

# Save the resulting DataFrame as a parquet file
filtered_bookings.write.mode("overwrite").parquet("C:/Users/mohit/Desktop/HRS/output/filtered_bookings.parquet")
spark.stop()
