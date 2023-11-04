package select

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object customer_referee {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        val data = Seq(
            Row(1, "Will", null),
            Row(2, "Jane", null),
            Row(3, "Alex", 2),
            Row(4, "Bill", null),
            Row(5, "Zack", 1),
            Row(6, "Mark", 2)
        )

        val schema = StructType(
            List(
                StructField(
                    name = "id",
                    dataType = IntegerType,
                    nullable = true
                ),
                StructField(
                    name = "name",
                    dataType = StringType,
                    nullable = true
                ),
                StructField(
                    name = "referee_id",
                    dataType = IntegerType,
                    nullable = true
                )
            )
        )

        val dataFrame = spark.createDataFrame(
            spark.sparkContext.parallelize(data),
            schema
        )

        dataFrame
          .select(
              col("name")
          )
          .where(
              col("referee_id") =!= 2 || col("referee_id").isNull
          ).show()

    }

}
