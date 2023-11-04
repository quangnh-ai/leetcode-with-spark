package basic_joins

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object employee_bonus {
    def main(args: Array[String]): Unit ={
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        val schemaEmployee = StructType(
            List(
                StructField(
                    name = "empId",
                    dataType = IntegerType,
                    nullable = true
                ),
                StructField(
                    name = "name",
                    dataType = StringType,
                    nullable = true
                ),
                StructField(
                    name = "supervisor",
                    dataType = IntegerType,
                    nullable = true
                ),
                StructField(
                    name = "salary",
                    dataType = IntegerType,
                    nullable = true
                )
            )
        )

        val schemaBonus = StructType(
            List(
                StructField(
                    name = "empId",
                    dataType = IntegerType,
                    nullable = true
                ),
                StructField(
                    name = "bonus",
                    dataType = IntegerType,
                    nullable = true
                )
            )
        )

        val dataEmployee = Seq(
            Row(3, "Brad", null, 4000),
            Row(1, "John", 3, 1000),
            Row(2, "Dan", 3, 2000),
            Row(4, "Thomas", 3, 4000)
        )

        val dataBonus = Seq(
            Row(2, 500),
            Row(4, 2000)
        )

        val employee = spark.createDataFrame(
            spark.sparkContext.parallelize(dataEmployee),
            schemaEmployee
        )

        val bonus = spark.createDataFrame(
            spark.sparkContext.parallelize(dataBonus),
            schemaBonus
        )

        employee
          .join(
              bonus,
              employee("empID") === bonus("empID"),
              "left"
          )
          .select(
              employee("name"),
              bonus("bonus")
          )
          .where(
              bonus("bonus") < 1000
              || bonus("bonus").isNull
          )
          .show()

    }
}
