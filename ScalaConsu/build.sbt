name := "ScalaConsu"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.2.0",
  "org.apache.kafka" % "kafka-streams" % "2.2.0",
  "org.slf4j" % "slf4j-simple" % "1.7.25",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "com.sendgrid" % "sendgrid-java" % "2.2.1",
  "com.typesafe" % "config" % "1.2.1",
  "org.apache.commons" % "commons-email" % "1.3.1",
)