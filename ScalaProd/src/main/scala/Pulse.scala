/*
Creating topic with 5 partitions

bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 5 --topic pulse


bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic pulse
bin/kafka-topics.sh --alter --bootstrap-server localhost:9092 --topic "pulse" --partitions 5

bin/kafka-topics.sh --describe --bootstrap-server localhost:9092
 */

import java.util.Properties
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import scala.util.control.Breaks._


object Pulse {
  def main(args: Array[String]): Unit = {

    new PulseProducer().process()
  }

  class PulseProducer() {

    def process(): Unit = {
      val props = new Properties()

      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

      val start = PulseProducer(100, props)

    }

    def PulseProducer(iter:Int, props:Properties): Int = {

      if (iter > 0) {

        val random = new scala.util.Random
        val key = 0 + random.nextInt((4 - 0) + 1)
        val value = getPValue(key).toString

        // save record data in
        val records = new ProducerRecord[String, String]("pulse", key.toString, value)

        // instantiate producer via props
        val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

        // send is asynchronous
        producer.send(records)
        println("Sending: " + value)
        Thread.sleep(1000)
        producer.close()
        //return
        PulseProducer(iter - 1, props)
      }
      0
    }

    def getPValue(key: Int): Int = {
      // the list is empty
      val random = new scala.util.Random
      val value = 40 + random.nextInt((130 - 40) + 1)
      value
    }
  }
}


