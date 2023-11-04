package aggregate_functions

import org.apache.spark.sql.functions.{avg, count, round, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object quality_and_percentage {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val queriesSchema = StructType(
            List(
                StructField(
                    name = "query_name",
                    dataType = StringType
                ),
                StructField(
                    name = "result",
                    dataType = StringType
                ),
                StructField(
                    name = "position",
                    dataType = IntegerType
                ),
                StructField(
                    name = "rating",
                    dataType = IntegerType
                )
            )
        )

        val dataQueries = Seq(
            Row("Dog", "Golden Retriever", 1, 5),
            Row("Dog", "German Shepherd", 2, 5),
            Row("Dog", "Mule", 200, 1),
            Row("Cat", "Shirazi", 5, 2),
            Row("Cat", "Siamese", 3, 3),
            Row("Cat", "Sphynx", 7, 4)
        )

        val queries = spark.createDataFrame(
            spark.sparkContext.parallelize(dataQueries),
            queriesSchema
        )

        queries
          .groupBy(
              queries("query_name")
          )
          .agg(
              round(
                  avg(queries("rating") / queries("position")),
                  2
              ).alias("quality"),
              round(
                  count(when(queries("rating") < 3, 1)) / count(queries("rating")),
                  2
              ).alias("poor_query_percentage")
          )
          .show()
    }
}
