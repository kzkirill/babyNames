import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by Kirill on 12/24/2016.
  */
object PageRate {

  def theLongest(s: String): String = {
    s.split("[0-9]")
      .filter(str => str.exists(ch => ch.isUpper))
      .maxBy(str => str.length) }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() setAppName ("Page Rate") setMaster ("local[4]")

    val sc = new SparkContext(conf)
    val links = sc.parallelize[(String, String)](List(("www.link1.com", "www.link12.com www.link11.com www.link21.com www.link41.com"),
      ("www.link2.com", "www.link124.com www.link151.com www.link621.com www.link141.com")))

    var ranks = links.mapValues(v => (v, 1.0))

    ranks foreach (println _)
  }

}
