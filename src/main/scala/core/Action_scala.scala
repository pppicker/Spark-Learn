package core

import org.apache.spark.{SparkConf, SparkContext}

object Action_scala {
  def main(args: Array[String]): Unit = {
//    reduce()
//    collect()
//    count()
//    take()
//    saveAsTextFile()
    countByKey()
  }
  def reduce(): Unit ={
    val conf = new SparkConf()
      .setAppName("reduce")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data = List(1,2,3,4,5)
    val rdd = sc.parallelize(data)
    val sum = rdd.reduce(_ + _)
    println(sum)
  }
  def collect(): Unit ={
    val conf = new SparkConf()
      .setAppName("collect")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data = List(1,2,3,4,5)
    val rdd = sc.parallelize(data)
    val doubleRDD = rdd.map(x => x * 2)
    val result = doubleRDD.collect()
    result.foreach(x => println(x))
  }
  def count(): Unit ={
    val conf = new SparkConf()
      .setAppName("count")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data = List(1,2,3,4,5)
    val rdd = sc.parallelize(data)
    val result = rdd.count()
    println(result)
  }

  def take(): Unit ={
    val conf = new SparkConf()
      .setAppName("take")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data = List(1,2,3,4,5)
    val rdd = sc.parallelize(data)
    val result = rdd.take(3)
    result.foreach(x => println(x))
  }

  def saveAsTextFile(): Unit ={
    val conf = new SparkConf()
      .setAppName("saveAsTextFile")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data = List(1,2,3,4,5)
    val rdd = sc.parallelize(data)
    rdd.saveAsTextFile("C:\\Users\\Administrator.PC-20180301AKPZ\\Desktop\\jdk\\spark_result")
  }

  def countByKey(): Unit ={
    val conf = new SparkConf()
      .setAppName("countByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val data = List(
      Tuple2[String, String]("class1", "张三"),
      Tuple2[String, String]("class1", "李四"),
      Tuple2[String, String]("class1", "王五"),
      Tuple2[String, String]("class1", "马六"),
      Tuple2[String, String]("class2", "赵琦"),
      Tuple2[String, String]("class2", "琳琳"),
      Tuple2[String, String]("class2", "丽丽")
    )
    val rdd = sc.parallelize(data)
    val result = rdd.countByKey()
    println(result)
    result.foreach(x => println(x._1 + " " + x._2))
  }

}




