package core

import org.apache.spark.{SparkConf, SparkContext}

object TopN_scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("topN")
      .setMaster("local")
    val sc = new SparkContext(conf)

//    val data = List(1,2,3,4,5,6,7,8,9)
//
//    val rdd = sc.parallelize(data)
//    val pair = rdd.map(x => (x, x))
//    val sorted_pair = pair.sortByKey(false)
//    val sorted_result = sorted_pair.map(x => x._2)
//    val top3 = sorted_result.take(3)
//    top3.foreach(x => println(x))

    val data = List(
      Tuple2[String, Integer]("class1", 88),
      Tuple2[String, Integer]("class1", 23),
      Tuple2[String, Integer]("class1", 33),
      Tuple2[String, Integer]("class1", 44),
      Tuple2[String, Integer]("class1", 77),
      Tuple2[String, Integer]("class2", 66),
      Tuple2[String, Integer]("class2", 11),
      Tuple2[String, Integer]("class2", 61),
      Tuple2[String, Integer]("class2", 55),
      Tuple2[String, Integer]("class2", 75)
    )

    val rdd = sc.parallelize(data)
    val group_rdd = rdd.groupByKey()
    val top3_group = group_rdd.map(x => {
      val xx = x._1
      val yy = x._2
      (xx, yy.toList.sorted.reverse.take(3))
    })

    top3_group.foreach(x => {
      println(x._1)
      x._2.foreach(x => println(x))
    })



  }

}
