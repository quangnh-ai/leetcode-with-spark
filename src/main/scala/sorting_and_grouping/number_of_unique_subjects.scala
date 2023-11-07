import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}
import org.apache.spark.sql.functions.{col, countDistinct}

object number_of_unique_subjects {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val teacherSchema = StructType(
            List(
                StructField(
                    name = "teacher_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "subject_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "dept_id",
                    dataType = IntegerType
                )
            )
        )

        val teacherData = Seq(
            Row(1, 2, 3),
            Row(1, 2, 4),
            Row(1, 3, 3),
            Row(2, 1, 1),
            Row(2, 2, 1),
            Row(2, 3, 1),
            Row(2, 4, 1)
        )

        val teacher = spark.createDataFrame(
            spark.sparkContext.parallelize(teacherData),
            teacherSchema
        )

        teacher
          .groupBy(
              teacher("teacher_id")
          )
          .agg(
              countDistinct(teacher("subject_id")).alias("cnt")
          )
          .show()
    }
}
