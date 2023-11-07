import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, datediff, min, when, count, countDistinct, round}
import org.apache.spark.sql.types.{DateType, IntegerType, StructField, StructType}

import java.sql.Date

object game_play_analysis_iv {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("test")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val activitySchema = StructType(
            List(
                StructField(
                    name = "player_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "device_id",
                    dataType = IntegerType
                ),
                StructField(
                    name = "event_date",
                    dataType = DateType
                ),
                StructField(
                    name = "games_played",
                    dataType = IntegerType
                )
            )
        )

        val activityData = Seq(
            Row(1, 2, Date.valueOf("2016-03-01"), 5),
            Row(1, 2, Date.valueOf("2016-03-02"), 6),
            Row(2, 3, Date.valueOf("2017-06-25"), 1),
            Row(3, 1, Date.valueOf("2016-03-02"), 0),
            Row(3, 4 ,Date.valueOf("2018-07-03"), 5)
        )

        val activity = spark.createDataFrame(
            spark.sparkContext.parallelize(activityData),
            activitySchema
        )

        activity
          .alias("activity")
          .join(
              activity
                .groupBy(col("player_id"))
                .agg(
                    min(
                        col("event_date")
                    ).alias("first_login_date")
                )
                .alias("first_login"),
              col("activity.player_id") === col("first_login.player_id")
              && col("activity.event_date") =!= col("first_login.first_login_date"),
              "left"
          )
          .agg(
              round(
                  count(
                      when(
                          datediff(
                              col("activity.event_date"),
                              col("first_login.first_login_date")
                          ) === 1,
                          1
                      )
                  ) / countDistinct("activity.player_id"),
                  2
              ).alias("fraction")
          )
          .show()

//        val first_login = activity
//          .groupBy(col("player_id"))
//          .agg(
//              min(
//                  col("event_date")
//              ).alias("first_login")
//          )
//
//        activity
//          .alias("activity")
//          .join(
//              first_login.alias("first_login"),
//              col("activity.player_id") === col("first_login.player_id")
//              && col("activity.event_date") =!= col("first_login.first_login"),
//              "left"
//          )
//          .agg(
//              round(
//                  count(
//                      when(
//                          datediff(
//                              col("activity.event_date"),
//                              col("first_login.first_login")
//                          ) === 1,
//                          1
//                      )
//                  ) / countDistinct(col("activity.player_id")),
//                  2
//              ).alias("fraction")
//          )
//          .show()
    }
}
