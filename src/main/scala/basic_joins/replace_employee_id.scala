package basic_joins

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object replace_employee_id {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        val employeesSchema = StructType(
            List(
                StructField(
                    name = "id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "name",
                    dataType = StringType
                )
            )
        )

        val employeeUNISchema = StructType(
            List(
                StructField(
                    name = "id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "unique_id",
                    dataType = IntegerType
                )
            )
        )

        val employees = Seq(
            Row(1, "Alice"),
            Row(7, "Bob"),
            Row(11, "Meir"),
            Row(90, "Winston"),
            Row(3, "Jonathan")
        )

        val employeeUNI = Seq(
            Row(3, 1),
            Row(11, 2),
            Row(90, 3)
        )

        val employeesDataframe = spark.createDataFrame(
            spark.sparkContext.parallelize(employees),
            employeesSchema
        )

        val employeeUNIDataframe = spark.createDataFrame(
            spark.sparkContext.parallelize(employeeUNI),
            employeeUNISchema
        )

        employeesDataframe
          .join(
              employeeUNIDataframe,
              employeesDataframe("id") === employeeUNIDataframe("id"),
              "left"
          )
          .select(
              col("unique_id"),
              col("name")
          )
          .orderBy(
              col("name").asc
          )
          .show()

    }
}
