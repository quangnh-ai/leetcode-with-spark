package aggregate_functions
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, count, date_format, sum, when, lit}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

import java.sql.Date

object monthly_transactions_i {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val transactionsSchema = StructType(
            List(
                StructField(
                    name = "id",
                    dataType = IntegerType,
                ),
                StructField(
                    name = "country",
                    dataType = StringType
                ),
                StructField(
                    name = "state",
                    dataType = StringType
                ),
                StructField(
                    name = "amount",
                    dataType = IntegerType
                ),
                StructField(
                    name = "trans_date",
                    dataType = DateType
                )
            )
        )

        val transactionsData = Seq(
            Row(121, "US", "approved", 1000, Date.valueOf("2018-12-18")),
            Row(122, "US", "declined", 2000, Date.valueOf("2018-12-19")),
            Row(123, "US", "approved", 2000, Date.valueOf("2019-01-01")),
            Row(124, "DE", "approved", 2000, Date.valueOf("2019-01-07"))
        )

        val transactions = spark.createDataFrame(
            spark.sparkContext.parallelize(transactionsData),
            transactionsSchema
        )

        transactions
          .withColumn(
             "month",
              date_format(
                  col("trans_date"),
                  "yyyy-MM"
              )
          )
          .groupBy(
              col("month"),
              col("country")
          )
          .agg(
              count(lit(1)).alias("trans_count"),
              count(
                  when(
                      col("state") === "approved",
                      1
                  )
              ).alias("approved_count"),
              sum(col("amount")).alias("trans_total_amount"),
              sum(
                  when(
                      col("state") === "approved",
                      col("amount")
                  )
              ).alias("approved_total_amount")
          )
          .show()

    }
}
