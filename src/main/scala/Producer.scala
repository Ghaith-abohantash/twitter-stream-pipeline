import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, Callback, RecordMetadata}
import java.util.Properties

object Producer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "5")
    props.put(ProducerConfig.ACKS_CONFIG, "all")

    val producer = new KafkaProducer[String, String](props)

    val lines = scala.io.Source.fromFile("src/main/Tweets/boulder_flood_geolocated_tweets.json").getLines()

    for (line <- lines) {
      val record = new ProducerRecord[String, String]("tweets-stream", "key", line)

      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception != null) {
            println("Error while producing message: " + exception)
          }
        }
      })

      Thread.sleep(25)
    }

    producer.close()
  }
}