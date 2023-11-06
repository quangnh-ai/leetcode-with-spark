import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.types.{StructType, StructField, DateType, IntegerType}
import java.sql.Date

object immediate_food_delivery_ii {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val deliverySchema = StructType(
            List(
                StructField(
                    name = "delivery_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "customer_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "order_date",
                    dataType = DateType
                ),
                StructField(
                    name = "customer_pref_delivery_date",
                    dataType = DateType
                )
            )
        )

        val deliveryData = Seq(
            Row(1, 1, Date.valueOf("2019-08-01"), Date.valueOf("2019-08-02")),
            Row(2, 2, Date.valueOf("2019-08-02"), Date.valueOf("2019-08-02")),
            Row(3, 1, Date.valueOf("2018-08-11"), Date.valueOf("2019-08-12")),
            Row(4, 3, Date.valueOf("2019-08-24"), Date.valueOf("2019-08-24")),
            Row(5, 3, Date.valueOf("2019-08-21"), Date.valueOf("2019-08-22")),
            Row(6, 2, Date.valueOf("2019-08-11"), Date.valueOf("2019-08-13")),
            Row(7, 4, Date.valueOf("2019-08-09"), Date.valueOf("2019-08-09"))
        )

        val delivery = spark.createDataFrame(
            spark.sparkContext.parallelize(deliveryData),
            deliverySchema
        )

        delivery.show()

    }
}
