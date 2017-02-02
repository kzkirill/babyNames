import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Kirill on 10/26/2016.
  */
object SimpleApp extends TextFileContext {

  case class Entry(val year: String, val name: String, val county: String, val sex: String, val count: Int)

  def mapEntry(row: Array[String]) = Entry(row(0), row(1), row(2), row(3), row(4).toInt)

  def main(args: Array[String]): Unit = {
    //    def namesRDD = sc.textFile(fileName, 2).cache()

    lazy val rows = valuesSplitted

    //    lazy val davidValues = namesRDD.filter(line => line.contains("DAVID")).count()
    //    println("Lines with David: %s".format(davidValues))

    //    val count = rows.map(row => row(2)).distinct.count

    //    println("General No of rows:%s ".format(count))
    //    print("Example of line: ")
    //    println(rows.first)

    //    val rowsDavid = rows.filter(line => line(1).contains("DAVID"))
    //    println("Number of Davids: %s".format(rowsDavid.count()))
    //    println("Number of rows of David with count > 10 %s".format(rowsDavid.map(line => line(4).toInt > 10).distinct.count))
    //
    val entries = rows.map(mapEntry)
    println("Entry example %s".format(entries.first))

    def rbkFun(acc: Int, value: Int): Int = acc + value

    entries.map(entry => (entry.name -> entry.count)).reduceByKey(rbkFun).sortBy(-_._2) take (3) foreach (println _)

  }
}
