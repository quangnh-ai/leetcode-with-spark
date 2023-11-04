package aggregate_functions

import org.apache.spark.sql.functions.{col, count, round}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object percentage_of_users {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val usersSchema = StructType(
            List(
                StructField(
                    name = "user_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "user_name",
                    dataType = StringType
                )
            )
        )

        val registerSchema = StructType(
            List(
                StructField(
                    name = "contest_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "user_id",
                    dataType = IntegerType
                )
            )
        )

        val usersData = Seq(
            Row(6, "Alice"),
            Row(2, "Bob"),
            Row(7, "Alex")
        )

        val registerData = Seq(
            Row(215, 6),
            Row(209, 2),
            Row(208, 2),
            Row(210, 6),
            Row(208, 6),
            Row(209, 7),
            Row(209, 6),
            Row(215, 7),
            Row(208, 7),
            Row(210, 2),
            Row(207, 2),
            Row(210, 7)
        )

        val users = spark.createDataFrame(
            spark.sparkContext.parallelize(usersData),
            usersSchema
        )

        val registers = spark.createDataFrame(
            spark.sparkContext.parallelize(registerData),
            registerSchema
        )

        users
          .join(
              registers,
              users("user_id") === registers("user_id")
          )
          .groupBy(
              registers("contest_id"),
          )
          .agg(
              round((count(registers("user_id")) * 100) / users.count(), 2).alias("percentage")
          )
          .orderBy(
              col("percentage").desc,
              col("contest_id").asc
          )
          .show()

        registers.count()
    }
}
