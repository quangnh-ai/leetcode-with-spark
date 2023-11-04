package basic_joins

import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object average_time {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        val schema = StructType(
            List(
                StructField(
                    name = "machine_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "process_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "activity_type",
                    dataType = StringType
                ),
                StructField(
                    name = "timestamp",
                    dataType = DoubleType
                )
            )
        )

        val data = Seq(
            Row(0, 0, "start", 0.712),
            Row(0, 0, "end", 1.520),
            Row(0, 1, "start", 3.140),
            Row(0, 1, "end", 4.120),
            Row(1, 0, "start", 0.550),
            Row(1, 0, "end", 1.550),
            Row(1, 1, "start", 0.430),
            Row(1, 1, "end", 1.420),
            Row(2, 0, "start", 4.100),
            Row(2, 0, "end", 4.512),
            Row(2, 1, "start", 2.500),
            Row(2, 1, "end", 5.000)
        )

        val activity = spark.createDataFrame(
            spark.sparkContext.parallelize(data),
            schema
        )

        activity.as("a1")
          .join(
              activity.as("a2"),
              col("a1.machine_id") === col("a2.machine_id")
              && col("a1.process_id") === col("a2.process_id")
              && col("a1.activity_type") === "start"
              && col("a2.activity_type") === "end"
          )
          .groupBy(
              col("a1.machine_id")
          )
          .agg(
              avg(col("a2.timestamp") - col("a1.timestamp"))
          )
          .show()
    }
}
