/**
  * Created by Kirill on 12/10/2016.
  */
object SimpleApp2 extends TextFileContext {

  def main(args: Array[String]): Unit = {
    val parallel = sc.parallelize(1 to 9, 4)
    val collected = parallel mapPartitions (x => List(x.next).iterator) collect()
    collected.foreach(println _)

    val parallel2 = sc.parallelize('a' to 'k', 4)
    val indexed = parallel2.mapPartitionsWithIndex((index: Int, it: Iterator[Char]) => it.toList.map(c => index + "," + c).iterator)
    val collected2 = indexed.collect
    val mapped = indexed map (c => {
      val array = c.split(",")
      (array(0).toInt -> (array(1), 1))
    }) reduceByKey ((a, b) => (a._1 + b._1 , a._2 + b._2))

    mapped foreach (println _)

//    collected2 foreach (println _)

    val chars1 = sc.parallelize('a' to 'k')
    val chars2 = sc.parallelize('g' to 'z')
    chars1.union(chars2) foreach( print _)
    println(" 1 ")
    chars1.intersection(chars2) map (print)
    println(" 2 ")
    chars1.union(chars2).distinct.map(print)
    println(" 3 ")
  }
}
