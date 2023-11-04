package aggregate_functions

import org.apache.spark.sql.functions.{col, round, sum}
import org.apache.spark.sql.types.{DateType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.Date

object avg_selling_price {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val pricesSchema = StructType(
            List(
                StructField(
                    name = "product_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "start_date",
                    dataType = DateType
                ),
                StructField(
                    name = "end_date",
                    dataType = DateType
                ),
                StructField(
                    name = "price",
                    dataType = IntegerType
                )
            )
        )

        val unitsSoldSchema = StructType(
            List(
                StructField(
                    name = "product_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "purchase_date",
                    dataType = DateType
                ),
                StructField(
                    name = "units",
                    dataType = IntegerType
                )
            )
        )

        val pricesData = Seq(
            Row(1, Date.valueOf("2019-02-17"), Date.valueOf("2019-02-28"), 5),
            Row(1, Date.valueOf("2019-03-01"), Date.valueOf("2019-03-22"), 20),
            Row(2, Date.valueOf("2019-02-01"), Date.valueOf("2019-02-20"), 15),
            Row(2, Date.valueOf("2019-02-21"), Date.valueOf("2019-03-31"), 30),
            Row(3, Date.valueOf("2019-02-21"), Date.valueOf("2019-03-31"), 30)
        )

        val unitsSoldData = Seq(
            Row(1, Date.valueOf("2019-02-25"), 100),
            Row(1, Date.valueOf("2019-03-01"), 15),
            Row(2, Date.valueOf("2019-02-10"), 200),
            Row(2, Date.valueOf("2019-03-22"), 30)
        )

        val prices = spark.createDataFrame(
            spark.sparkContext.parallelize(pricesData),
            pricesSchema
        )

        val unitsSold = spark.createDataFrame(
            spark.sparkContext.parallelize(unitsSoldData),
            unitsSoldSchema
        )

        prices.alias("prices")
          .join(
              unitsSold.alias("unitsSold"),
              col("prices.product_id") === col("unitsSold.product_id")
              && col("unitsSold.purchase_date").between(
                  col("prices.start_date"),
                  col("prices.end_date")
              ),
              "left"
          )
          .groupBy(
              col("prices.product_id")
          )
          .agg(
              sum(
                  col("prices.price") * col("unitsSold.units")
              ).alias("total_revenue"),
              sum(
                  col("unitsSold.units")
              ).alias("total_units"),
              round(
                  (
                    col("total_revenue") / col("total_units")
                    ),
                  2
              ).alias("average_price")
          )
          .na.fill(0)
//          .withColumn(
//              "test",
//              col("prices.price") * col("unitsSold.units")
//          )
          .show()


    }
}
