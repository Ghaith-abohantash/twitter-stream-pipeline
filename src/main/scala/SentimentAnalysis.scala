import com.johnsnowlabs.nlp.base.DocumentAssembler
import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.annotators.Normalizer
import com.johnsnowlabs.nlp.annotators.sda.vivekn.ViveknSentimentModel
import com.johnsnowlabs.nlp.Finisher
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SentimentAnalysis {
  def processTweetSentiment(tweet: String, spark: SparkSession): String = {
    import spark.implicits._

    val tweetDF = Seq(tweet).toDF("tweet")

    val document = new DocumentAssembler()
      .setInputCol("tweet")
      .setOutputCol("document")

    val token = new Tokenizer()
      .setInputCols("document")
      .setOutputCol("token")

    val normalizer = new Normalizer()
      .setInputCols("token")
      .setOutputCol("normal")

    val vivekn = ViveknSentimentModel.pretrained()
      .setInputCols("document", "normal")
      .setOutputCol("result_sentiment")

    val finisher = new Finisher()
      .setInputCols("result_sentiment")
      .setOutputCols("final_sentiment")

    val pipeline = new Pipeline().setStages(Array(document, token, normalizer, vivekn, finisher))

    val pipelineModel = pipeline.fit(tweetDF)

    val result = pipelineModel.transform(tweetDF)

    val sentimentArray = result.select("final_sentiment").as[Seq[String]].collect()

    val sentiment = if (sentimentArray.nonEmpty) sentimentArray.head.head else "Unknown sentiment"

    sentiment
  }
}
