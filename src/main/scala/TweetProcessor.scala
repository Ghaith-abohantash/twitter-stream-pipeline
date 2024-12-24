import scala.util.matching.Regex

object TweetProcessor {
  def processTweet(tweet: String, timestamp: String, tweetId: String, geoCoordinates: Option[(Double, Double)]): String = {
    val stopWords = Set("the", "is", "and", "a", "of", "in")

    val cleanedTweet = tweet.split("\\s+")
      .filter(word => !word.startsWith("http") && !word.startsWith("@") && !word.startsWith("#"))
      .mkString(" ")

    val words = cleanedTweet.split("\\s+")
      .filter(word => !stopWords.contains(word.toLowerCase))

    val hashtagPattern: Regex = "#([A-Za-z0-9_]+)".r
    val hashtags = hashtagPattern.findAllIn(tweet).toList

    val mentionPattern: Regex = "@([A-Za-z0-9_]+)".r
    val mentions = mentionPattern.findAllIn(tweet).toList

    val result =
      s"""
         |Tweet ID: $tweetId
         |Tweet: $cleanedTweet
         |Keywords: ${words.mkString(", ")}
         |Hashtags: ${hashtags.mkString(", ")}
         |Mentions: ${mentions.mkString(", ")}
         |Coordinates: ${geoCoordinates.map(coord => s"Lat: ${coord._1}, Long: ${coord._2}").getOrElse("N/A")}
         |Timestamp: $timestamp
       """.stripMargin

    result
  }
}