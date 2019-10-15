package core

import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorVariable_scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("core.AccumulatorVariable")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data = List(1,2,3,4,5)
    val rdd = sc.parallelize(data)

    val acc = sc.accumulator(0)

    rdd.foreach(x => acc.add(x))

    println(acc.value)
  }
}
