import org.apache.spark.sql.SparkSession

/**
  * Created by Kirill on 1/6/2017.
  */
object SQLWithRDD {

  case class Person(name: String, age: Long)

  def main(args: Array[String]) = {
    val path = "C:/Users/Kirill/Documents/Spark/sparkBabyNames/src/main/resources/people.text"
    val spark = SparkSession.builder().appName("Basic SQL Example").master("local[4]") getOrCreate()

    import spark.implicits._

    val peopleDF = spark.sparkContext.
      textFile(path).map(line => line.split(",")).
                    map(attribute => Person(attribute(0), attribute(1).trim.toLong)).toDF

    peopleDF.printSchema()
    peopleDF.show

  }
}
