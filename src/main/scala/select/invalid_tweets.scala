package select

import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object invalid_tweets {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        val schema = StructType(
            List(
                StructField(
                    name = "tweet_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "content",
                    dataType = StringType
                )
            )
        )

        val data = Seq(
            Row(1, "Vote for Biden"),
            Row(2, "Let us make America great again!")
        )

        val dataFrame = spark.createDataFrame(
            spark.sparkContext.parallelize(data),
            schema
        )

        dataFrame
          .select(
              col("tweet_id")
          )
          .where(
              length(col("content")) > 15
          )
          .show()

    }
}
