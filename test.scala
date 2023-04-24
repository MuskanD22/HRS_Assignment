import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object test extends App {

  /*val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name","Word Count")
  sparkConf.set("spark.master","local[2]")

  val spark=SparkSession.builder().config(sparkConf).getOrCreate()

  val sc=spark.sparkContext;
  //1. loading the data in memory in form of rdd
  val loadDataRDD = sc.textFile("C:/Users/mohit/Desktop/BigDataCourse/week 10/6.datasets/samplefile.txt")
  //res0: Array[String] = Array(spark is very intersting, spark is inmemoery compute engine, "", I hope you are learning well, I hope your learning experence with trendytech is great, "")

  //split the sentense of word of array flat map basically take each line as input
  val wordRdd=loadDataRDD.flatMap(x=> x.split(" ")) //array(array{hello, how, id},array--)

  val assignNumberToWordRdd= wordRdd.map(x=> (x,1))  //(hello,1),() ----

  val reduceToFindCountRdd= assignNumberToWordRdd.reduceByKey((x,y)=> x+y) //In Spark, the reduceByKey function is a frequently used transformation operation that performs aggregation of data. It receives key-value pairs (K, V) as an input,
                                                                          //aggregates the values based on the key and generates a dataset of (K, V) pairs as an output.

  reduceToFindCountRdd.collect().foreach(println) //action

 */
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Booking ETL")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  // read the bookings data into a DataFrame
  val bookings = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
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