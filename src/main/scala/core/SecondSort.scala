package core

import org.apache.spark.{SparkConf, SparkContext}

object SecondSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SecondSort")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data = List(
      Tuple2[Integer, Integer](3, 3),
      Tuple2[Integer, Integer](1, 1),
      Tuple2[Integer, Integer](3, 2),
      Tuple2[Integer, Integer](2, 3),
      Tuple2[Integer, Integer](2, 1),
      Tuple2[Integer, Integer](1, 2),
      Tuple2[Integer, Integer](3, 1),
      Tuple2[Integer, Integer](2, 2),
      Tuple2[Integer, Integer](1, 3)
    )
    val rdd = sc.parallelize(data)

    val key_rdd = rdd.map(x => (new SecondSortKey(x._1, x._2),x))
    val sorted_key_rdd = key_rdd.sortByKey()
    val result = sorted_key_rdd.map(x => (x._2._1, x._2._2))
    result.foreach(x => {
      println(x._1 + " " + x._2)
    })
  }

}
