package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.rand


class BaseConstructionBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])]) extends Construction with Serializable {
  /*
  * Initialize LSH data structures here
  * You need to broadcast the data structures to all executors and use them locally
  * */

//  val lookUpTable = data.flatMap(x => x._2).distinct().zipWithIndex().map{ case(tag, index:Long) => tag -> index.toInt}
//  val shuffledDF = sqlContext.createDataFrame(lookUpTable).orderBy(rand())
//  val broadcastLookUpTable = sqlContext.sparkSession.sparkContext.broadcast(shuffledDF)


  val lookUpTable = data.flatMap(x => x._2).distinct().zipWithIndex().map{ case(tag, index:Long) => tag -> index.toInt}.collectAsMap()
  val broadcastLookUpTable = sqlContext.sparkSession.sparkContext.broadcast(lookUpTable)

  private def computeMinHash(rdd: RDD[(String, List[String])]): RDD[(String, Int)] = rdd.map { case (movie, tags) => (movie, tags.map(tag => broadcastLookUpTable.value.get(tag) match {
    case Some(p) => p
    case None => Int.MaxValue
  }).reduceLeft(math.min))}


  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    /*
    * This method performs a near-neighbor computation for the data points in rdd against the data points in data.
    * You need to perform the queries by using LSH with min-hash.
    * The perturbations needs to be consistent - decided once and randomly for each BaseConstructor object
    * sqlContext: current SQLContext
    * data: data points in (movie_name, [keyword_list]) format to compare against
    * rdd: data points in (movie_name, [keyword_list]) format that represent the queries
    * return near-neighbors in (movie_name, [nn_movie_names]) as an RDD[(String, Set[String])]
    * */

    val hashMinRdd = computeMinHash(rdd)
    val hashMinData = computeMinHash(data)

    hashMinRdd.cartesian(hashMinData)
      .filter { case ((rddMovie, minRdd), (dataMovie, minData)) => minRdd == minData }
      .map { case ((rddMovie, _), (dataMovie, _)) => (rddMovie, dataMovie) }
      .groupBy(x => x._1)
      .map { case (movie, list) => (movie, list.map(_._2).toSet) }
  }
}
