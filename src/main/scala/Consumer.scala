import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import java.util.Properties
import scala.collection.JavaConverters._
import play.api.libs.json._
import org.apache.spark.sql.SparkSession
import SentimentAnalysis._
import TweetProcessor._
import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestHighLevelClient, RequestOptions}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.xcontent.XContentType

object Consumer {
  def main(args: Array[String]): Unit = {
    val stopWords = Set("the", "and", "is", "in", "to", "for", "of", "on", "at", "a", "an", "with", "this", "that", "it", "by", "as", "from")

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

    val client = new RestHighLevelClient(
      RestClient.builder(new HttpHost("localhost", 9200, "http"))
    )

    try {
      while (true) {
        val records = consumer.poll(1000)
        for (record <- records.asScala) {
          try {
            val tweetJson = Json.parse(record.value())

            val tweet = (tweetJson \ "text").asOpt[String].getOrElse("Unknown Tweet")
            val timestamp = (tweetJson \ "created_at").asOpt[String].getOrElse("Unknown Time")
            val tweetId = (tweetJson \ "id_str").asOpt[String].getOrElse("Unknown ID")

            val hashtags = (tweetJson \ "entities" \ "hashtags")
              .asOpt[JsArray]
              .map(_.value.map(hashtag => (hashtag \ "text").as[String]).toList)
              .getOrElse(List.empty[String])

            val mentions = (tweetJson \ "entities" \ "user_mentions")
              .asOpt[JsArray]
              .map(_.value.map(mention => (mention \ "screen_name").as[String]).toList)
              .getOrElse(List.empty[String])

            val words = tweet.split("\\s+")
            val keywords = words.filter(word => !stopWords.contains(word.toLowerCase))

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

            val jsonData = JsObject(
              Seq(
                "tweet" -> JsString(tweet),
                "sentiment" -> JsString(sentiment),
                "timestamp" -> JsString(timestamp),
                "location" -> JsString(location.getOrElse("N/A")),
                "geo_coordinates" -> geoCoordinates.map { coord =>
                  JsObject(Seq("lat" -> JsNumber(coord._1), "lon" -> JsNumber(coord._2)))
                }.getOrElse(JsNull),
                "hashtags" -> JsArray(hashtags.map(JsString)),
                "mentions" -> JsArray(mentions.map(JsString)),
                "keywords" -> JsArray(keywords.map(JsString))
              )
            )

            val indexRequest = new IndexRequest("tweets_index")
              .id(tweetId)
              .source(jsonData.toString(), XContentType.JSON)

            val response = client.index(indexRequest, RequestOptions.DEFAULT)
            println(s"Document indexed with ID: ${response.getId}")

          } catch {
            case e: Exception =>
              println(s"Error processing tweet: ${e.getMessage}")
          }
        }
      }
    } finally {
      consumer.close()
      client.close()
    }
  }
}
