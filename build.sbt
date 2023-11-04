ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "leetcode_with_spark"
  )
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.4.1",
    "org.apache.spark" %% "spark-sql" % "3.4.1",
    "org.apache.spark" % "spark-streaming_2.12" % "3.4.1" % "provided"
)