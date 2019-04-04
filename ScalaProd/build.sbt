name := "ScalaProd"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.2.0",
  "org.apache.kafka" % "kafka-streams" % "2.2.0",
  "org.slf4j" % "slf4j-simple" % "1.7.25",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "org.apache.hadoop" % "hadoop-client" % "2.7.3",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "2.4.1",
  "org.apache.spark" % "spark-core_2.12" % "2.4.1",
  "org.apache.spark" % "spark-streaming_2.12" % "2.4.1"
)