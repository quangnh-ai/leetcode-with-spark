package aggregate_functions

import org.apache.spark.sql.functions.{avg, round}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object project_employees_i {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val projectSchema = StructType(
            List(
                StructField(
                    name = "project_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "employee_id",
                    dataType = IntegerType
                )
            )
        )

        val employeeSchema = StructType(
            List(
                StructField(
                    name = "employee_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "name",
                    dataType = StringType
                ),
                StructField(
                    name = "experience_years",
                    dataType = IntegerType
                )
            )
        )

        val projectData = Seq(
            Row(1, 1),
            Row(1, 2),
            Row(1, 3),
            Row(2, 1),
            Row(2, 4)
        )

        val employeeData = Seq(
            Row(1, "Khaled", 3),
            Row(2, "Ali", 2),
            Row(3, "John", 1),
            Row(4, "Doe", 2)
        )

        val project = spark.createDataFrame(
            spark.sparkContext.parallelize(projectData),
            projectSchema
        )

        val employee = spark.createDataFrame(
            spark.sparkContext.parallelize(employeeData),
            employeeSchema
        )

        project
          .join(
              employee,
              project("employee_id") === employee("employee_id"),
              "left"
          )
          .groupBy(
              project("project_id")
          )
          .agg(
              round(
                  avg(employee("experience_years")),
                  2
              ).alias("average_years")
          )
          .show()

    }
}
