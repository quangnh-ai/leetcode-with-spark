package basic_joins

import org.apache.spark.sql.functions.{col, count, round, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.Timestamp

object confirmation_rate {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        val signupsSchema = StructType(
            List(
                StructField(
                    name = "user_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "time_stamp",
                    dataType = TimestampType
                )
            )
        )

        val confirmationsSchema = StructType(
            List(
                StructField(
                    name = "user_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "time_stamp",
                    dataType = TimestampType
                ),
                StructField(
                    name = "action",
                    dataType = StringType
                )
            )
        )

        val dataSignups = Seq(
            Row(3, Timestamp.valueOf("2020-03-21 10:16:13")),
            Row(7, Timestamp.valueOf("2020-01-04 13:57:59")),
            Row(2, Timestamp.valueOf("2020-07-29 23:09:44")),
            Row(6, Timestamp.valueOf("2020-12-09 10:39:37"))
        )

        val dataConfirmations = Seq(
            Row(3, Timestamp.valueOf("2021-01-06 03:30:46"), "timeout"),
            Row(3, Timestamp.valueOf("2021-07-14 14:00:00"), "timeout"),
            Row(7, Timestamp.valueOf("2021-06-12 11:57:29"), "confirmed"),
            Row(7, Timestamp.valueOf("2021-06-13 12:58:28"), "confirmed"),
            Row(7, Timestamp.valueOf("2021-06-14 13:59:27"), "confirmed"),
            Row(2, Timestamp.valueOf("2021-01-22 00:00:00"), "confirmed"),
            Row(2, Timestamp.valueOf("2021-02-28 23:59:59"), "timeout")
        )

        val signups = spark.createDataFrame(
            spark.sparkContext.parallelize(dataSignups),
            signupsSchema
        )

        val confirmations = spark.createDataFrame(
            spark.sparkContext.parallelize(dataConfirmations),
            confirmationsSchema
        )
//
//        val totalRequest = signups
//          .join(
//              confirmations,
//              signups("user_id") === confirmations("user_id"),
//              "left"
//          )
//          .groupBy(
//              signups("user_id")
//          )
//          .agg(
//              count(signups("user_id")).alias("total_request")
//          )
//
//        val confirmedRequest = confirmations
//          .groupBy(
//              col("user_id"),
//              col("action")
//          )
//          .agg(
//              count(col("user_id")).alias("confirmed_request")
//          )
//          .where(
//              col("action") === "confirmed"
//          )
//
//        totalRequest.alias("totalRequest")
//          .join(
//              confirmedRequest.alias("confirmedRequest"),
//              col("totalRequest.user_id") === col("confirmedRequest.user_id"),
//              "left"
//          )
//          .na.fill(
//            0,
//            Seq("confirmedRequest.confirmed_request")
//          )
//          .select(
//              col("totalRequest.user_id"),
////              col("confirmed_request.action"),
//              round(
//                  col("confirmed_request") / col("total_request"),
//                  2
//              ).alias("ratio")
//          )
//          .show()

        signups
          .join(
              confirmations,
              signups("user_id") === confirmations("user_id"),
              "left"
          )
          .groupBy(
              signups("user_id")
          )
          .agg(
              count(confirmations("action")).alias("total_request"),
              count(
                  when(
                      confirmations("action") === "confirmed",
                      1,
                  )
              ).alias("confirmed_request")
          )
          .withColumn(
              "ratio",
              round(
                  col("confirmed_request") / col("total_request"),
                  2
              )
          )
          .na.fill(0)
          .show()

    }
}
