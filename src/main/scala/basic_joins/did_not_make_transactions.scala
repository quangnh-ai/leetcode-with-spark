package basic_joins

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object did_not_make_transactions {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        val visitsSchema = StructType(
            List(
                StructField(
                    name = "visit_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "customer_id",
                    dataType = IntegerType
                )
            )
        )

        val transactionsSchema = StructType(
            List(
                StructField(
                    name = "transaction_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "visit_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "amount",
                    dataType = IntegerType
                )
            )
        )

        val visits = Seq(
            Row(1, 23),
            Row(2, 9),
            Row(4, 30),
            Row(5, 54),
            Row(6, 96),
            Row(7, 54),
            Row(8, 54)
        )

        val transactions = Seq(
            Row(2, 5, 310),
            Row(3, 5, 300),
            Row(9, 5, 200),
            Row(12, 1, 910),
            Row(13, 2, 970)
        )

        val visitsDataframe = spark
          .createDataFrame(
              spark.sparkContext.parallelize(visits),
              visitsSchema
          )

        val transactionsDataframe = spark
          .createDataFrame(
              spark.sparkContext.parallelize(transactions),
              transactionsSchema
          )

        visitsDataframe
          .join(
              transactionsDataframe,
              visitsDataframe("visit_id") === transactionsDataframe("visit_id"),
              "left"
          )
          .groupBy(
              visitsDataframe("customer_id"),
              transactionsDataframe("transaction_id"),
//              transactionsDataframe("visit_id")
          )
          .count()
          .where(
              transactionsDataframe("transaction_id").isNull
          )
          .select(
              col("customer_id"),
              col("count").alias("count_no_trans")
          )
          .show()
    }
}
