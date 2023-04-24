from pyspark.sql.functions import col, concat, lit, date_add, when, concat_ws
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BookingsETL").getOrCreate()

# Read the bookings data
bookings_df = spark.read.csv("C:/Users/mohit/Desktop/HRS/Question2_Input.csv", header=True, inferSchema=True)

# Filter bookings with Tour Operators as Market Segment designation
tour_operators_bookings = bookings_df.filter(col("market_segment") == "Tour Operators")

tour_operators_bookings = tour_operators_bookings.withColumn("departure_date",
                   date_add(concat_ws("-", col("arrival_date_year"), col("arrival_date_month"), col("arrival_date_day_of_month")),
                            (col("stays_in_weekend_nights") + col("stays_in_week_nights")).cast(IntegerType()))
                  ).withColumn("with_family_breakfast", when((col("children")+col("babies"))>0, "Yes").otherwise("No"))




  #  .withColumn("arrival_date", concat_ws("-", col("arrival_date_year"), col("arrival_date_month"), col("arrival_date_day_of_month")))\
   # .withColumn("departure_date", date_add(concat_ws("-", col("arrival_date_year"), col("arrival_date_month"), col("arrival_date_day_of_month")), (col("stays_in_weekend_nights") + col("stays_in_week_nights")).cast(IntegerType())))\


# Add arrival_date and departure_date fields
#tour_operators_bookings = tour_operators_bookings.withColumn("arrival_date", concat(col("arrival_date_year"), lit("-"), col("arrival_date_month"), lit("-"), col("arrival_date_day_of_month")))
#tour_operators_bookings = tour_operators_bookings.withColumn("departure_date", date_add(col("arrival_date"), col("stays_in_weekend_nights") + col("stays_in_week_nights")))

# Add with_family_breakfast field
#with_family_breakfast = when(col("children") + col("babies") > 0, "Yes").otherwise("No")
#tour_operators_bookings = tour_operators_bookings.withColumn("with_family_breakfast", with_family_breakfast)

# Save the resulting dataset as a parquet file
tour_operators_bookings.write.mode("overwrite").parquet("C:/Users/mohit/Desktop/HRS/output/filtered_bookings.parquet")

import unittest
from pyspark.sql import SparkSession
from booking import filtered_bookings


class TestBookingsETL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("BookingsETL").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_filter_bookings(self):
        bookings = self.spark.createDataFrame([(1, "Tour Operators"), (2, "Corporate"), (3, "Tour Operators")],
                                              ["booking_id", "MarketSegment"])
        filtered_booking_test = filtered_bookings(bookings)
        expected_output = [(1, "Tour Operators")]