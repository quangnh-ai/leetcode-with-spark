package basic_joins

import org.apache.spark.sql.functions.{col, date_add}
import org.apache.spark.sql.types.{DateType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.Date

object rising_temperature {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("spark://localhost:7077")
          .config("spark.executor.memory", "2G")
          .config("spark.executor.cores", "2")
          .config("spark.driver.memory", "2G")
          .config("spark.driver.cores", "2")
          .appName("test")
          .getOrCreate()

        val schema = StructType(
            List(
                StructField(
                    name = "id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "recordDate",
                    dataType = DateType
                ),
                StructField(
                    name = "temperatureDate",
                    dataType = IntegerType
                )
            )
        )

        val data = Seq(
            Row(1, Date.valueOf("2015-01-01"), 10),
            Row(2, Date.valueOf("2015-01-02"), 25),
            Row(3, Date.valueOf("2015-01-03"), 20),
            Row(4, Date.valueOf("2015-01-04"), 30)
        )

        val weather = spark.createDataFrame(
            spark.sparkContext.parallelize(data),
            schema
        )

        weather.as("w1")
          .join(
              weather.as("w2"),
              col("w1.recordDate") === date_add(col("w2.recordDate"), 1)
          )
          .select(col("w1.id"))
          .where(
              col("w1.temperatureDate") > col("w2.temperatureDate")
          )
          .show()
    }
}
