/*
kafka stream consume and produce
https://itnext.io/a-cqrs-approach-with-kafka-streams-and-scala-49bfa78e4295

bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 5 --topic filtered

bin/kafka-topics.sh --alter --bootstrap-server localhost:9092 --topic filtered-pulse --partitions 5
 */
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}

object ScalaStream {
  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "pulse-filter-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)

    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val builder = new StreamsBuilder()
    val value = 0
    val textLines = builder.stream[String, String]("pulse")

    val uppedvalue = textLines
      .filter((k: String, v: String) => v.toFloat > 100)


    uppedvalue.to("filtered-pulse")

    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

  }
}