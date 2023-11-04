package basic_joins

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object product_sales_analysis_i {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        val salesSchema = StructType(
            List(
                StructField(
                    name = "sale_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "product_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "year",
                    dataType = IntegerType
                ),
                StructField(
                    name = "quantity",
                    dataType = IntegerType
                ),
                StructField(
                    name = "price",
                    dataType = IntegerType
                )
            )
        )

        val productSchema = StructType(
            List(
                StructField(
                    name = "product_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "product_name",
                    dataType = StringType
                )
            )
        )

        val sales = Seq(
            Row(1, 100, 2008, 10, 5000),
            Row(2, 100, 2009, 12, 5000),
            Row(7, 200, 2011, 15, 9000)
        )

        val product = Seq(
            Row(100, "Nokia"),
            Row(200, "Apple"),
            Row(300, "Samsung")
        )

        val salesDataframe = spark.createDataFrame(
            spark.sparkContext.parallelize(sales),
            salesSchema
        )
        val productDataframe = spark.createDataFrame(
            spark.sparkContext.parallelize(product),
            productSchema
        )

        salesDataframe
          .join(
              productDataframe,
              salesDataframe("product_id") === productDataframe("product_id")
          )
          .select(
              productDataframe("product_name"),
              salesDataframe("year"),
              salesDataframe("price")
          )
          .show()

    }
}
