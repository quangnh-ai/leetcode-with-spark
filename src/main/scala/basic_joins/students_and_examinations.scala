package basic_joins

import org.apache.spark.sql.functions.count
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object students_and_examinations {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        val studentsSchema = StructType(
            List(
                StructField(
                    name = "student_id",
                    dataType = IntegerType,
                ),
                StructField(
                    name = "student_name",
                    dataType = StringType
                )
            )
        )

        val examinationsSchema = StructType(
            List(
                StructField(
                    name = "student_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "subject_name",
                    dataType = StringType
                )
            )
        )

        val subjectsSchema = StructType(
            List(
                StructField(
                    name = "subject_name",
                    dataType = StringType
                )
            )
        )

        val studentsData = Seq(
            Row(1, "Alice"),
            Row(2, "Bob"),
            Row(13, "John"),
            Row(6, "Alex")
        )

        val subjectsData = Seq(
            Row("Math"),
            Row("Physics"),
            Row("Programming")
        )

        val examinationsData = Seq(
            Row(1, "Math"),
            Row(1, "Physics"),
            Row(1, "Programming"),
            Row(2, "Programming"),
            Row(1, "Physics"),
            Row(1, "Math"),
            Row(13, "Math"),
            Row(13, "Programming"),
            Row(13, "Physics"),
            Row(2, "Math"),
            Row(1, "Math")
        )

        val students = spark.createDataFrame(
            spark.sparkContext.parallelize(studentsData),
            studentsSchema
        )

        val subjects = spark.createDataFrame(
            spark.sparkContext.parallelize(subjectsData),
            subjectsSchema
        )

        val examinations = spark.createDataFrame(
            spark.sparkContext.parallelize(examinationsData),
            examinationsSchema
        )

        students
          .crossJoin(
              subjects
          )
          .join(
              examinations,
              subjects("subject_name") === examinations("subject_name")
              && students("student_id") === examinations("student_id"),
              "left"
          )
          .groupBy(
              students("student_id"),
              students("student_name"),
              subjects("subject_name")
          )
          .agg(
              count(examinations("student_id"))
          )
          .orderBy(
              students("student_id").asc,
              students("student_name").asc,
              subjects("subject_name").asc
          )
          .show()
    }
}
