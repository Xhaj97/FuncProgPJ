import sbt.Keys.libraryDependencies

name := "ScalaSpark"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,

  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"

)
