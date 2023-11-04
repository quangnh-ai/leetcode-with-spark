package select

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object big_countries {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        val schema = StructType(
            List(
                StructField(
                    name = "name",
                    dataType = StringType
                ),
                StructField(
                    name = "continent",
                    dataType = StringType
                ),
                StructField(
                    name = "area",
                    dataType = IntegerType
                ),
                StructField(
                    name = "population",
                    dataType = IntegerType
                ),
                StructField(
                    name = "gdp",
                    dataType = LongType
                )
            )
        )

        val data = Seq(
            Row("Afghanistan", "Asia", 652230, 25500100, 20343000000L),
            Row("Albania", "Europe", 28748, 2831741, 20343000000L),
            Row("Algeria", "Africa", 2381741, 37100000, 188681000000L),
            Row("Andorra", "Europe", 468, 78115, 3712000000L),
            Row("Angola", "Africa", 1246700, 20609294, 100990000000L)
        )

        val dataFrame = spark.createDataFrame(
            spark.sparkContext.parallelize(data),
            schema
        )

        dataFrame
          .select(
              col("name"),
              col("population"),
              col("area")
          )
          .where(
              col("area") >= 3000000 || col("population") >= 25000000
          )
          .show()
    }
}
