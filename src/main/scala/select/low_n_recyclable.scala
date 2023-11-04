package select

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object low_n_recyclable {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        val schema = StructType(
            List(
                StructField(
                    name="product_id",
                    dataType=IntegerType
                ),
                StructField(
                    name="low_fats",
                    dataType=StringType
                ),
                StructField(
                    name="recyclable",
                    dataType=StringType
                )
            )
        )

        val data = Seq(
            Row(0, "Y", "N"),
            Row(1, "Y", "Y"),
            Row(2, "N", "Y"),
            Row(3, "Y", "Y"),
            Row(4, "N", "N")
        )

        val dataFrame = spark.createDataFrame(
            spark.sparkContext.parallelize(data),
            schema
        )

        dataFrame
          .select(
              col("product_id")
          )
          .where(
              col("low_fats") === "Y" && col("recyclable") === "Y"
          )
          .show()
    }
}
