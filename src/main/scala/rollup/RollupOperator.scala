package rollup

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}

class RollupOperator() extends Serializable {

  private def aggRollUp(datasetG: RDD[(List[Any],Iterable[Row])],aggAttributeIndex: Int,agg : String):  RDD[(List[Any], Double)] = agg match {
    case "COUNT" => datasetG.map { case (key, data) => (key, data.size.toDouble) }
    case "SUM" => datasetG.map { case (key, data) => (key, data.foldLeft(0.0) { (acc, curr) => acc + curr.getAs[Int](aggAttributeIndex) }) }
    case "MIN" => datasetG.map { case (key, data) => (key, data.reduceLeft { (a, b) => if (a.getAs[Int](aggAttributeIndex) < b.getAs[Int](aggAttributeIndex)) a else b }.getAs[Int](aggAttributeIndex).toDouble) }
    case "MAX" => datasetG.map { case (key, data) => (key, data.reduceLeft { (a, b) => if (a.getAs[Int](aggAttributeIndex) < b.getAs[Int](aggAttributeIndex)) b else a }.getAs[Int](aggAttributeIndex).toDouble) }
    case "AVG" => datasetG.map { case (key, data) => (key, data.foldLeft(0.0) { (acc, curr) => acc + curr.getAs[Int](aggAttributeIndex) } / data.size.toDouble) }
  }

  /*
 * This method gets as input one dataset, the indexes of the grouping attributes of the rollup (ROLLUP clause)
 * the index of the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = List[Any], value = Double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */

  def rollup(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double)] = {
    val datasetG = groupingAttributeIndexes.indices.foldRight(dataset.groupBy(row => groupingAttributeIndexes.map(i => row(i)))) { (index, acc) =>
      acc.filter(_._1.length == index + 1).groupBy(_._1.slice(0, index)).map { case (key, data) => // length == index + 1 to only use (n+1)-length groupings to compute (n)-length groupings
        key -> data.flatMap(x => x._2)
      }.union(acc)
    }
    aggRollUp(datasetG,aggAttributeIndex,agg)
  }

  def rollup_naive(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double)] = {
    (0 to groupingAttributeIndexes.size).toList.map { index =>
      val datasetG = dataset.groupBy(row => groupingAttributeIndexes.slice(0, index).map(i => row(i)))
      aggRollUp(datasetG,aggAttributeIndex,agg)
    }.reduce((a, b) => a.union(b))
  }
}
