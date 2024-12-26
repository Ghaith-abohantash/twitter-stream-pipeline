import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import java.util.Properties
import scala.collection.JavaConverters._
import play.api.libs.json._
import org.apache.spark.sql.SparkSession
import SentimentAnalysis._
import TweetProcessor._

object Consumer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "tweet-group")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(java.util.Collections.singletonList("tweets-stream"))

    val spark = SparkSession.builder()
      .appName("SentimentAnalysis")
      .master("local[*]")
      .config("spark.local.dir", "C:/spark-temp")
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
      .getOrCreate()

    try {
      while (true) {
        val records = consumer.poll(1000)
        for (record <- records.asScala) {
          try {
            val tweetJson = Json.parse(record.value())

            val tweet = (tweetJson \ "text").asOpt[String].getOrElse("Unknown Tweet")
            val timestamp = (tweetJson \ "created_at").asOpt[String].getOrElse("Unknown Time")
            val tweetId = (tweetJson \ "id_str").asOpt[String].getOrElse("Unknown ID")

            val geoCoordinates = (tweetJson \ "geo" \ "coordinates").asOpt[Seq[Double]]
              .flatMap {
                case Seq(long, lat) => Some((lat, long))
                case _ => None
              }

            val location = (tweetJson \ "place" \ "full_name").asOpt[String]
              .orElse((tweetJson \ "user" \ "location").asOpt[String])

            val sentiment = SentimentAnalysis.processTweetSentiment(tweet, spark)

            val processedTweet = TweetProcessor.processTweet(tweet, timestamp, tweetId, geoCoordinates, location)

            println(s"$processedTweet\nSentiment: $sentiment\n")
          } catch {
            case e: Exception =>
              println(s"Error processing tweet: ${e.getMessage}")
          }
        }
      }
    } finally {
      consumer.close()
    }
  }
}
