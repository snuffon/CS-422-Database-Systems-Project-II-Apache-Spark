package thetajoin

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.{Logger, LoggerFactory}
import scala.util.Sorting

class ThetaJoin(partitions: Int) extends java.io.Serializable {
  val logger: Logger = LoggerFactory.getLogger("ThetaJoin")
  val r = partitions.toDouble

  private def assign(num: Int, bounds: IndexedSeq[Int]): Int = {
    var result = bounds.size
    var i = 0
    while (result == bounds.size && i < bounds.size) {
      if (bounds(i) > num)
        result = i
      i += 1
    }
    result
  }
  /* this method takes as input two datasets (dat1, dat2) and returns the pairs of join keys that satisfy the theta join condition.
  attrIndex1: is the index of the join key of dat1 attrIndex2: is the index of the join key of dat2 condition: Is the join condition.
  Assume only "<", ">" will be given as input Assume condition only on integer fields. Returns and RDD[(Int, Int)]: projects only the join keys. */
  def ineq_join(dat1: RDD[Row], dat2: RDD[Row], attrIndex1: Int, attrIndex2: Int, condition: String): RDD[(Int, Int)] = {
    val t0 = System.nanoTime()
    val d1 = dat1.map(row => row(attrIndex1).asInstanceOf[Int])
    val d2 = dat2.map(row => row(attrIndex2).asInstanceOf[Int])
    val dim1 = d1.collect().length
    val dim2 = d2.collect().length
    val c1 = dim1 / math.sqrt(dim1 * dim2 / r).toInt
    val c2 = dim2 / math.sqrt(dim1 * dim2 / r).toInt
    val s1n = math.min(dim1, c1)     // can be tuned
    val s2n = math.min(dim2, c2)
    val s1 = d1.takeSample(withReplacement = false, s1n).sorted
    val s2 = d2.takeSample(withReplacement = false, s2n).sorted
    val bounds1 = (1 until c1).map(i => s1(math.ceil((i / c1.toDouble) * s1n).toInt))
    val bounds2 = (1 until c2).map(i => s2(math.ceil((i / c2.toDouble) * s2n).toInt))
    val t1 = System.nanoTime()
    //println ((t1 - t0) / 1000000)
    condition match {
      case "<" => {
        d1.cartesian(d2).groupBy(x => {     //groupBy region
          (assign(x._1, bounds1), assign(x._2, bounds2))
        }).filter(r => {      //filter non qualifiying regions
          if (r._1._1 == 0 || r._1._2 == c2 - 1)
            true
          else if (bounds1(r._1._1 - 1) < bounds2(r._1._2))
            true
          else false
        }).flatMap { x => if (x._2.map(pair => pair._1).max < x._2.map(pair => pair._2).min)      //prune comparisons
          x._2
        else
          x._2.filter(y => y._1 < y._2) }
      }
      case ">" => {     // same logic inverted
        d1.cartesian(d2).groupBy(x => {
          (assign(x._1, bounds1), assign(x._2, bounds2))
        }).filter(r => {
          if (r._1._2 == 0 || r._1._1 == c1 - 1)
            true
          else if (bounds2(r._1._2 - 1)
            < bounds1(r._1._1)) true
          else
            false
        }).flatMap { x => if (x._2.map(pair => pair._2).max < x._2.map(pair => pair._1).min)
          x._2
        else
          x._2.filter(y => y._2 < y._1) }
      }
    }
  }
}