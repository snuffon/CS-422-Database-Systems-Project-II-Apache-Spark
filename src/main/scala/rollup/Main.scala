package rollup

import org.apache.spark.sql.functions._
import java.io._

import org.apache.spark.sql.{Row, SQLContext, SparkSession}

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Project2")
      .master("local[*]")
      .getOrCreate()


    val input = new File(getClass.getResource("/lineorder_small.tbl").getFile).getPath
    //val input = new File(getClass.getResource("/lineorder_small3K.tbl").getFile).getPath
    //val input = new File(getClass.getResource("/lineorder_small1K.tbl").getFile).getPath

    val df = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(input)

    val rdd = df.rdd

    //val groupingList = List(0)
    val groupingList = List(0, 1)
    //val groupingList = List(0, 1, 3)


    val rollup = new RollupOperator


    var tot = 0.0
    for (_ <- 1 to 500) {
      val t0 = System.nanoTime()

      //val res = rollup.rollup_naive(rdd, groupingList, 8, "SUM")
      val res = rollup.rollup(rdd, groupingList, 8, "SUM")

      //res.foreach(x => println(x))

      val t1 = System.nanoTime()
      tot += (t1 - t0) / 1000000
    }
    println(tot / 500)
    // use the following code to evaluate the correctness of your results
    val correctRes = df.rollup("lo_orderkey", "lo_linenumber", "lo_partkey").agg(sum("lo_quantity")).rdd
      .map(row => (row.toSeq.toList.dropRight(1).filter(x => x != null), row(row.size - 1)))
    //correctRes.foreach(x => println(x))
  }
}