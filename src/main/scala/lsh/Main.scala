package lsh

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import lsh.ExactNN


object Main {
  def recall(ground_truth: RDD[(String, Set[String])], lsh_truth: RDD[(String, Set[String])]): Double = {
    /*
    * Compute the recall for each near-neighbor LSH query against the accurate result
    * Then, compute the average across all queries
    * ground_truth: results of queries in (movie_name, [nn_movie_names]) format produced by ExactNN
    * lsh_truth: results of queries in (movie_name, [nn_movie_names]) format produced by an LSH Construction
    * returns average recall
    * */

    //Recall is the percentage of the neighbors that were retrieved.
    //Recall = true positive / (true positive + false negative)


    val truePositive: Double = ground_truth.join(lsh_truth).map { case (movie, (truth, lsh)) => lsh.foldLeft(0.0)((acc, curr) => if (truth.contains(curr)) acc + 1 else acc) }.collect().sum
    val falseNegative: Double = ground_truth.join(lsh_truth).map { case (movie, (truth, lsh)) => (truth -- lsh).size }.collect().sum
    truePositive / (truePositive.toDouble + falseNegative)

  }

  def precision(ground_truth: RDD[(String, Set[String])], lsh_truth: RDD[(String, Set[String])]): Double = {
    /*
    * Compute the precision for each near-neighbor LSH query against the accurate result
    * Then, compute the average across all queries
    * ground_truth: results of queries in (movie_name, [nn_movie_names]) format produced by ExactNN
    * lsh_truth: results of queries in (movie_name, [nn_movie_names]) format produced by an LSH Construction
    * returns average precision
    * */

    //Precision is the percentage of correctly retrieved results (they are indeed neighbors)
    //Precision = true positive / (true positive + false positive)
    val truePositive: Double = ground_truth.join(lsh_truth).map { case (movie, (truth, lsh)) => lsh.foldLeft(0.0)((acc, curr) => if (truth.contains(curr)) acc + 1 else acc) }.collect().sum
    val falsePositive: Double = ground_truth.join(lsh_truth).map { case (movie, (truth, lsh)) => lsh.foldLeft(0.0)((acc, curr) => if (!truth.contains(curr)) acc + 1 else acc) }.collect().sum
    truePositive / (truePositive.toDouble + falsePositive)
  }

  def query1(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/lsh-query-1.csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val exact: Construction = new ExactNN(sqlContext, rdd_corpus, 0.3)

    val lsh: Construction = new ORConstruction(List(new ANDConstruction(List(new BaseConstruction(sqlContext, rdd_corpus), new BaseConstruction(sqlContext, rdd_corpus))),
      new ANDConstruction(List(new BaseConstruction(sqlContext, rdd_corpus), new BaseConstruction(sqlContext, rdd_corpus)))))
    val t0 = System.nanoTime()
    val ground = exact.eval(rdd_query)
    val t1 = System.nanoTime()
    val res = lsh.eval(rdd_query)
    val t2 = System.nanoTime()



    println("ground :")
    println("q1 : recall : " + recall(ground, res) + " ,aim : 0.7")
    println("q1 : precision : " + precision(ground, res) + " ,aim : 0.98")
    println("q1 : time : " + (t1-t0)/1000000 + "ms")

    println("res :")
    println("q1 : recall : " + recall(ground, res))
    println("q1 : precision : " + precision(ground, res))
    println("q1 : time : " + (t2-t1)/1000000 + "ms")

    println("q1 : done")

    //assert(recall(ground, res) > 0.7)
    //assert(precision(ground, res) > 0.98)
  }

  def query2(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/lsh-query-2.csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val exact: Construction = new ExactNN(sqlContext, rdd_corpus, 0.3)

    val lsh: Construction = new ORConstruction(List(new ANDConstruction(List(new BaseConstruction(sqlContext, rdd_corpus), new BaseConstruction(sqlContext, rdd_corpus))),
      new ANDConstruction(List(new BaseConstruction(sqlContext, rdd_corpus), new BaseConstruction(sqlContext, rdd_corpus)))))

    val t0 = System.nanoTime()
    val ground = exact.eval(rdd_query)
    val t1 = System.nanoTime()
    val res = lsh.eval(rdd_query)
    val t2 = System.nanoTime()



    println("q2 : recall : " + recall(ground, res) + " ,aim : 0.9")
    println("q2 : precision : " + precision(ground, res) + " ,aim : 0.45")
    //assert(recall(ground, res) > 0.9)
    //assert(precision(ground, res) > 0.45)

    println("q2 : time : " + (t1-t0)/1000000 + "ms")

    println("res :")
    println("q2 : recall : " + recall(ground, res))
    println("q2 : precision : " + precision(ground, res))
    println("q2 : time : " + (t2-t1)/1000000 + "ms")

    println("q2 : done")

  }

  def query0(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/lsh-query-0.csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val exact: Construction = new ExactNN(sqlContext, rdd_corpus, 0.3)

    val lsh: Construction = new ORConstruction(List(new ANDConstruction(List(new BaseConstruction(sqlContext, rdd_corpus), new BaseConstruction(sqlContext, rdd_corpus))),
      new ANDConstruction(List(new BaseConstruction(sqlContext, rdd_corpus), new BaseConstruction(sqlContext, rdd_corpus)))))

    val t0 = System.nanoTime()
    val ground = exact.eval(rdd_query)
    val t1 = System.nanoTime()
    val res = lsh.eval(rdd_query)
    val t2 = System.nanoTime()

    println("q0 : recall : " + recall(ground, res) + " ,aim : 0.83")
    println("q0 : precision : " + precision(ground, res) + " ,aim : 0.7")
    //assert(recall(ground, res) > 0.83)
    //assert(precision(ground, res) > 0.70)

    println("q0 : time : " + (t1-t0)/1000000 + "ms")

    println("res :")
    println("q0 : recall : " + recall(ground, res))
    println("q0 : precision : " + precision(ground, res))
    println("q0 : time : " + (t2-t1)/1000000 + "ms")

    println("q0 : done")

  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    query0(sc, sqlContext)
    query1(sc, sqlContext)
    query2(sc, sqlContext)
  }
}
