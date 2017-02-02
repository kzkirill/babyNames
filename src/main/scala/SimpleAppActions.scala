/**
  * Created by Kirill on 12/15/2016.
  */
object SimpleAppActions extends TextFileContext {
  def main(args: Array[String]) = {
    val names1 = sc.parallelize(List("abby", "bobby", "rudy"))
    println("All names combined:" + names1.reduce((str1, str2) => str1 + " " + str2))
    println("Size of all names 1 : " + names1.flatMap(name => List(name.size)).reduce((s1, s2) => s1 + s2))

    val names2 = sc.parallelize(List("apple", "beatty", "beattrice")) map (name => (name, name.size))
    names2 foreach (tuple => println(tuple._1 + " is of size " + tuple._2))
    println("Combined size of names 2 : " + names2.flatMap(t => Array(t._2)).reduce(_ + _))
  }
}
