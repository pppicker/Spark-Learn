package core

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastVariable_scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("BroadcastVariable")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data = List(1,2,3,4,5)
    val rdd = sc.parallelize(data)

    val factor = 3;
    val Broadcast_factor = sc.broadcast(factor)

    val result = rdd.map(x => x * Broadcast_factor.value)
    result.foreach( x => println(x))
  }

}
