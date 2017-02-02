import SimpleApp.{fileName, sc}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Kirill on 12/10/2016.
  */
trait TextFileContext {
  val fileName = "C:/Users/Kirill/Documents/Spark/sparkBabyNames/src/main/resources/baby-names.csv"
  val conf = new SparkConf() setAppName ("Babys' Names") setMaster ("local[4]")

  val sc = new SparkContext(conf)

  def valuesSplitted = {
    sc.textFile(fileName).map(line => line.split(",")).filter(row => !row(0).contains("Year"))
  }

}
