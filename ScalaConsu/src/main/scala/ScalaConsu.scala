object ScalaConsu {
  def main(args: Array[String]): Unit = {
    import java.util.Properties
    import java.time.Duration
    import java.util

    import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer, ConsumerConfig}
    import org.apache.kafka.common.serialization.StringDeserializer

    import scala.collection.JavaConverters._
    import Mail._


    // instantiate the properties
    val props = new Properties()

    // telling the machine that kafka will run at the address localhost, on port 9092
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    // telling the machine that the key and the value of the message will be of type String
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "get_pulse_A")
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor")
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100")

    // instantiate the consumer
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

    // consumer will follow a topic
    consumer.subscribe(util.Collections.singletonList("filtered-pulse"))

    // fetching the result. Waiting for a message if no msg
    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))

      for (record <- records.asScala) {
        println("La machine " + record.key() + " a levé une alert: pulse = " + record.value())
        if ((record.key()) == "1") {
          send a new Mail(
            from = ("an address", "YourPatch"),
            to = "an address",
            subject = "ALERT Pulse",
            message = "Your patch number " + record.key() + " raise an alert! Your pulse reached " + record.value() + "!"
          )
        }
      }
    }
  }
}
