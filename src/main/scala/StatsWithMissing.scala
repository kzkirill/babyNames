import org.apache.spark.util.StatCounter

class NAStatCounter extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double) = {
    if (java.lang.Double.isNaN(x))
      missing += 1
    else
      stats.merge(x)
    this
  }

  def merge(other: NAStatCounter) = {
    this.stats.merge(other.stats)
    this.missing += other.missing
    this
  }

  override def toString = {
    "Stats: " + stats.toString + " mising: " + missing
  }
}

object NAStatCounter extends Serializable {
  def apply(x: Double) = new NAStatCounter().add(x)
}

def statsWithMissing(rdd: org.apache.spark.rdd.RDD[Array[Double]]):
Array[NAStatCounter] = {
  val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {
    val nas: Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))
    iter.foreach(arr => {
      nas.zip(arr).foreach { case (n, d) => n.add(d) }
    }
    )
    Iterator(nas)
  }
  )
  nastats.reduce((n1, n2) => {
    n1.zip(n2)
  }.map { case (a, b) => a.merge(b) })
}
