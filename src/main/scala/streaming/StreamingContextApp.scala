package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds

/**
  * Created by Kirill on 1/15/2017.
  */
object StreamingContextApp {
  def main(args: Array[String]) = {
    val conf = new SparkConf() setAppName("Old context streaming") setMaster ("local[4]")

    val context = new StreamingContext(conf, Seconds(1))
    val lines = context.socketTextStream("localhost",7777)

    val errorLines = lines.filter(_.contains("error"))
    errorLines.print()
  }
}
