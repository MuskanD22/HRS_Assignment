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
