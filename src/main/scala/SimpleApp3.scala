/**
  * Created by Kirill on 12/12/2016.
  */
object SimpleApp3 extends TextFileContext {

  def main(arg: Array[String]): Unit = {
    val rows = valuesSplitted
    //    val grouped = rows.map(line => (line(1), line(2)))

    rows.map(row => (row(1), row(4))).aggregateByKey(0)((k, v) => v.toInt + k, (a, b) => a + b)
  }

}
