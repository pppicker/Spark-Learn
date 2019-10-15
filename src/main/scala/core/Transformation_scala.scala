package core

import org.apache.spark.{SparkConf, SparkContext}

object Transformation_scala {

  def main(args: Array[String]): Unit = {
//    map()
//    filter()
//    flatmap()
//    groupByKey()
//    reduceByKey()
//    sortByKey()
//    join()
     cogroup()
  }


  def map(): Unit ={
    val conf = new SparkConf()
      .setAppName("map")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val data = List(1,2,3,4,5)
    val numRDD = sc.parallelize(data)
    val numDoubleRDD = numRDD.map(x => x * 2)
    numDoubleRDD.foreach(x => println(x))
  }

  def filter(): Unit ={
    val conf = new SparkConf()
      .setAppName("filter")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val data = List(1,2,3,4,5)
    val numRDD = sc.parallelize(data)

    val result = numRDD.filter(x => x%2==0)
    result.foreach(x => println(x))
  }
  def flatmap(): Unit ={
    val conf = new SparkConf()
      .setAppName("flatmap")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val data = List("hello world! today is good day!", "java scala spark hadoop streaming kafka etl python spider")
    val rdd = sc.parallelize(data)
    val result = rdd.flatMap(line => line.split(" "))
    result.foreach(x => println(x))
  }
  def groupByKey(): Unit ={
    val conf = new SparkConf()
      .setAppName("groupByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data = List(
      Tuple2("class1", 90),
      Tuple2("class1", 22),
      Tuple2("class1", 30),
      Tuple2("class1", 66),
      Tuple2("class2", 88),
      Tuple2("class2", 99),
      Tuple2("class2", 100)
    )
    val tupleRDD = sc.parallelize(data)

    val result = tupleRDD.groupByKey()
    result.foreach(x => {
      println(x._1)
      x._2.foreach{x => println(x)}
      println("===============================")
    })
  }
  def reduceByKey(): Unit ={
    val conf = new SparkConf()
      .setAppName("reduceByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data = List(
      Tuple2("class1", 90),
      Tuple2("class1", 22),
      Tuple2("class1", 30),
      Tuple2("class1", 66),
      Tuple2("class2", 88),
      Tuple2("class2", 99),
      Tuple2("class2", 100)
    )
    val tupleRDD = sc.parallelize(data)

    val result = tupleRDD.reduceByKey(_ + _)
    result.foreach(x => println(x._1 + x._2))
  }

  def sortByKey(): Unit ={
    val conf = new SparkConf()
      .setAppName("sortByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data = List(
      Tuple2("张三", 90),
      Tuple2("李四", 22),
      Tuple2("王五", 30),
      Tuple2("马六", 66),
      Tuple2("赵琦", 88),
      Tuple2("王八", 99),
      Tuple2("徐九", 100)
    )
    val tupleRDD = sc.parallelize(data)
    val revertTupleRDD = tupleRDD.map(t => (t._2,t._1))

    val result = revertTupleRDD.sortByKey(false)
    result.foreach(x => println(x._1 + " " + x._2))

  }
  def join(): Unit ={
    val conf = new SparkConf()
      .setAppName("join")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data1 = List(
      Tuple2[Integer, Integer](1, 45),
      Tuple2[Integer, Integer](2, 75),
      Tuple2[Integer, Integer](3, 64),
      Tuple2[Integer, Integer](4, 90),
      Tuple2[Integer, Integer](1, 55)
    )
    val data2 = List(
      Tuple2[Integer, String](1, "张三"),
      Tuple2[Integer, String](2, "李四"),
      Tuple2[Integer, String](3, "王五"),
      Tuple2[Integer, String](4, "马六")
    )
    val tupleRDD1 = sc.parallelize(data1)
    val tupleRDD2 = sc.parallelize(data2)

    val result = tupleRDD1.join(tupleRDD2)

    result.foreach(x => println(x._1 + " " + x._2))

  }

  def cogroup(): Unit ={
    val conf = new SparkConf()
      .setAppName("cogroup")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data1 = List(
      Tuple2[Integer, Integer](1, 45),
      Tuple2[Integer, Integer](2, 75),
      Tuple2[Integer, Integer](3, 64),
      Tuple2[Integer, Integer](4, 90),
      Tuple2[Integer, Integer](1, 55)
    )
    val data2 = List(
      Tuple2[Integer, String](1, "张三"),
      Tuple2[Integer, String](2, "李四"),
      Tuple2[Integer, String](3, "王五"),
      Tuple2[Integer, String](4, "马六")
    )
    val tupleRDD1 = sc.parallelize(data1)
    val tupleRDD2 = sc.parallelize(data2)

    val result = tupleRDD1.cogroup(tupleRDD2)
    val result_sorted = result.sortByKey(false)

    result_sorted.foreach(x => println(x._1 + " " + x._2))

  }

}
