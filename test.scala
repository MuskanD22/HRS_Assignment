import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object test extends App {
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Booking ETL")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  // read the bookings data into a DataFrame
  val bookings = spark.read.format("csv")
    .option("header", "true")
    .load("C:/Users/mohit/Desktop/HRS/Question2_Input.csv")

  // filter the bookings made by Tour Operators
  val tourOperatorBookings = bookings.filter(col("market_segment") === "Tour Operators")

  // calculate arrival and departure dates
  val bookingsWithDates = tourOperatorBookings.withColumn(
    "arrival_date",
    to_date(concat(col("arrival_date_year"), lit("-"), col("arrival_date_month"), lit("-"), col("arrival_date_day_of_month")), "yyyy-MM-dd"))
    .withColumn(
      "departure_date",
      expr("date_add(arrival_date, CAST(stays_in_weekend_nights AS INT) + CAST(stays_in_week_nights AS INT))"))

  // determine if booking includes family breakfast
  val bookingsWithFamilyBreakfast = bookingsWithDates.withColumn(
    "with_family_breakfast",
    when(col("children") + col("babies") > 0, "Yes").otherwise("No"))

  // save the result as a parquet file
  bookingsWithFamilyBreakfast.write.parquet("C:/Users/mohit/Desktop/HRS/output/file.parquet")
  spark.stop()

}
