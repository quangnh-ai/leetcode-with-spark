package basic_joins

import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object manager_with_5_reports {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
//          .master("spark://localhost:7077")
          .appName("test")
//          .config("spark.executor.memory", "2g")
//          .config("spark.driver.memory", "2g")
//          .config("spark.executor.cores", "2")
//          .config("spark.driver.cores", "2")
          .getOrCreate()

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
                    name = "department",
                    dataType = StringType,
                    nullable = true
                ),
                StructField(
                    name = "managerId",
                    dataType = IntegerType,
                    nullable = true
                )
            )
        )

        val data = Seq(
            Row(101, "John", "A", null),
            Row(102, "Dan", "A", 101),
            Row(103, "James", "A", 101),
            Row(104, "Amy", "A", 101),
            Row(105, "Anne", "A", 101),
            Row(106, "Ron", "B", 101)
        )

        val employee = spark.createDataFrame(
            spark.sparkContext.parallelize(data),
            schema
        )

//        employee
//          .groupBy(
//              col("managerId")
//          )
//          .count()
//          .where(
//              col("managerId").isNotNull
//          ).alias("report_count")
//          .join(
//              employee.alias("e1"),
//              col("report_count.managerId") === col("e1.id")
//          )
//          .select("e1.name")
//          .show()

        val report_count = employee
          .groupBy(col("managerId"))
          .agg(count(col("managerId")).alias("direct_report"))
          .where(
              col("managerId").isNotNull
              && col("direct_report") >= 5
          )


        report_count.show()

        report_count
          .alias("report_count")
          .join(
              employee.alias("employee"),
              col("report_count.managerId") === col("employee.id")
          )
          .select(
              col("id"),
              col("name"),
              col("direct_report")
          )
          .show()
    }
}
