/*

bin/kafka-topics.sh --create --bootstrap-server  localhost:9092 --replication-factor 1 --partitions 5 --topic patch

*/

import java.util.Properties
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

object Patch {
  def main(args: Array[String]): Unit = {
    new PatchProducer().process()
  }
}


class PatchProducer() {

  def process(): Unit = {
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val start = PatchProducer(100, props)

  }

  def PatchProducer(key:Int, props:Properties): Int = {

    if (key > 0) {

      val lat = getLatValue()
      val lon = getLonValue()
      val temp = getTempValue()
      val power = getBatteryValue()
      val status = getStatus(temp, power)

      val value = "{\"id\": " + key + ",\"lat\":" + lat.toString + ",\"long\":" + lon.toString + ",\"temperature\":" + temp.toString + ",\"power\":" + power.toString + ",\"status\":" +"\""+ status + "\"}"

      // instantiate producer via props
      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

      // save record data in
      val records = new ProducerRecord[String, String]("patch", key.toString, value)

      // send is asynchronous
      producer.send(records)
      println("Sending: " + value)
      Thread.sleep(500)
      producer.close()
      //return
      PatchProducer(key - 1, props)
    }
    0
  }

  def getStatus(temp:Float, power:Int): String = {
    if (power == 0) "off"
    else if (temp > 40 || temp < -5) "off"
    else "on"
  }

  def getLatValue(): Float = {
    val random = new scala.util.Random
    if ((random.nextInt(2) % 2) == 0) {
      val lat = 0 + random.nextFloat() * 90
      lat
    }

    else {
      val lat = -(0 + random.nextFloat() * 90)
      lat
    }
  }

  def getLonValue(): Float = {
    val random = new scala.util.Random
    if ((random.nextInt(2) % 2) == 0) {
      val lon = 0 + random.nextFloat() * 90
      lon
    }

    else {
      val lon = -(0 + random.nextFloat() * 90)
      lon
    }
  }

  def getTempValue(): Float = {
    val random = new scala.util.Random
    if ((random.nextInt(2) % 2) == 0) {
      val temp = 0 + random.nextFloat() * 35
      temp
    }
    else {
      val temp = 0 + random.nextFloat() * 35 - 10
      temp
    }
  }

  def getBatteryValue(): Int = {
    val random = new scala.util.Random
    if ((random.nextInt(7) % 7) == 0) {
      val battery = 0
      battery
    }
    else {
      val battery = 0 + random.nextInt(10) * 7
      battery
    }
  }
}
