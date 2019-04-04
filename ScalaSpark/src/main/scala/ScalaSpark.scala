import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}


object ScalaSpark {
  def main(args: Array[String]): Unit = {
    new StreamsProcessor("localhost:9092").process()
  }
}

class StreamsProcessor(brokers: String) {

  def process(): Unit = {

    val spark = SparkSession.builder()
      .appName("Kafka-getter")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // reading coordinate kafka data
    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", "patch")
      .load()

    val patchJsonDf = inputDf.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("id", DataTypes.StringType)
      .add("lat", DataTypes.StringType)
      .add("long", DataTypes.StringType)
      .add("temperature", DataTypes.StringType)
      .add("power", DataTypes.StringType)
      .add("status", DataTypes.StringType)

    val patchNestedDf = patchJsonDf.select(from_json($"value", struct).as("patch"))

    val patchFlattenedDf = patchNestedDf.selectExpr("patch.id", "patch.lat", "patch.long", "patch.temperature", "patch.power","patch.status")

    val query = patchFlattenedDf.writeStream
      .outputMode("append")
      .format("json")
      .option("path", "your path")
      .option("checkpointLocation", "your path")
      .trigger(Trigger.ProcessingTime("40 seconds")) //time to wait before getting the data from the stream
      .start()

    query.awaitTermination()
  }

}
