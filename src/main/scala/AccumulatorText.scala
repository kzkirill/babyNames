/**
  * Created by Kirill on 12/30/2016.
  */
object AccumulatorText extends TextFileContext {

  def main(args: Array[String]): Unit = {

    val emptyLines = sc.longAccumulator("emptylines")
    val file = sc.textFile(fileName)
    val callSigns = file.flatMap(line => {
      if (line == "") {
        emptyLines add 1
      }
      line.split(" ")
    })

    callSigns.saveAsTextFile("C:/Users/Kirill/Documents/Spark/sparkBabyNames/src/main/resources/output.txt")
    println("Blank lines: " + emptyLines.value)
  }
}
