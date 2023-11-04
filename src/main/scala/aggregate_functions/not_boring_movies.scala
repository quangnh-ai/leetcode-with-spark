package aggregate_functions

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object not_boring_movies {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        val cinemaSchema = StructType(
            List(
                StructField(
                    name = "id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "movie",
                    dataType = StringType
                ),
                StructField(
                    name = "description",
                    dataType = StringType
                ),
                StructField(
                    name = "rating",
                    dataType = DoubleType
                )
            )
        )

        val cinemaData = Seq(
            Row(1, "War", "great 3D", 8.9),
            Row(2, "Science", "fiction", 8.5),
            Row(3, "irish", "boring", 6.2),
            Row(4, "Ice song", "Fantacy", 8.6),
            Row(5, "House card", "Interesting", 9.1)
        )

        val cinema = spark.createDataFrame(
            spark.sparkContext.parallelize(cinemaData),
            cinemaSchema
        )

        cinema
          .where(
              cinema("id") % 2 === 1
              && cinema("description") =!= "boring"
          )
          .sort(
              cinema("rating").desc
          )
          .show()

    }
}
