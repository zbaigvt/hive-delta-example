name := "WorkingwithHive"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.5.1" % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % "1.5.1" % "provided",
  "org.apache.spark" % "spark-hive_2.11" % "1.5.1" % "provided",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.3"
)
