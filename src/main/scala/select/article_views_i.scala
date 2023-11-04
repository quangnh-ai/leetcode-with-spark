package select

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.Date

object article_views_i {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        val schema = StructType(
            List(
                StructField(
                    name = "article_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "author_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "viewer_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "view_date",
                    dataType = DateType
                )
            )
        )

        val data = Seq(
            Row(1, 3, 5, Date.valueOf("2019-08-01")),
            Row(1, 3, 6, Date.valueOf("2019-08-02")),
            Row(2, 7, 7, Date.valueOf("2019-08-01")),
            Row(2, 7, 6, Date.valueOf("2019-08-02")),
            Row(4, 7, 1, Date.valueOf("2019-07-22")),
            Row(3, 4, 4, Date.valueOf("2019-07-21")),
            Row(3, 4, 4, Date.valueOf("2019-07-21"))
        )

        val dataFrame = spark.createDataFrame(
            spark.sparkContext.parallelize(data),
            schema
        )

        dataFrame
          .select(
              col("author_id").alias("id")
          ).distinct()
          .where(
              col("viewer_id") === col("author_id")
          )
          .sort(
              col("id").asc
          )
          .show()
    }
}
