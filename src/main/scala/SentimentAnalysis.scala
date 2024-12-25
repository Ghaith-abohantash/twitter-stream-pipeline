import com.johnsnowlabs.nlp.base.DocumentAssembler
import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.annotators.Normalizer
import com.johnsnowlabs.nlp.annotators.sda.vivekn.ViveknSentimentModel
import com.johnsnowlabs.nlp.Finisher
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}

object SentimentAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NLP")
      .master("local[*]")
      .config("spark.local.dir", "F:/spark-temp")
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
      .getOrCreate()

    import spark.implicits._


   def  processTweets(rawTweets: DataFrame, spark: SparkSession): Unit = {
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

    val pipelineModel = pipeline.fit(rawTweets)

    val result = pipelineModel.transform(rawTweets)

    result.select("tweet", "final_sentiment").show(false)
  }
}
