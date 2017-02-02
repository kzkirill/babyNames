/**
  * Created by Kirill on 1/4/2017.
  */

import org.apache.spark.sql.SparkSession

object SQLApplication {

  case class Tweet(text: String, retweetCount: BigInt)


  def main(args: Array[String]) = {
    val path = "C:/Users/Kirill/Documents/Spark/sparkBabyNames/src/main/resources/testweet.json"
    val spark = SparkSession.builder().appName("Basic SQL Example").master("local[4]") getOrCreate()

    import spark.implicits._

    val df = spark.read.json(path)
    df.show

    df.createOrReplaceTempView("tweets")

    val topTweetsDF = spark.sql("SELECT text, retweetCount, isFavorited " +
      "FROM tweets " +
      "ORDER BY retweetCount LIMIT 10")
    topTweetsDF.show

    df.printSchema()

    df.select("text").show
    df.select($"text", $"retweetCount" + 1).show()

    df.filter($"retweetCount" === 0).show()
    df.groupBy("retweetCount").count.show

    val tweetsDS = Seq(Tweet("Chirp chirp", 23),Tweet("Chirp chirp", 23),Tweet("Chirp chirp", 23),
      Tweet("Chirasasdp ", 2),Tweet("Chirp asds", 2),Tweet("Chirp asasd", 2)).toDS
    tweetsDS.show
    tweetsDS.groupBy($"retweetCount").count.show()

    val primitiveDS = Seq(1, 4, 56).toDS
    primitiveDS.show

    val tweetsAllDS = spark.read.json(path).as[Tweet]
    tweetsAllDS.show

  }

}
