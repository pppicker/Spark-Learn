package core

import org.apache.spark.{SparkConf, SparkContext}

object WordCountLocal_scala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("wordCount")
    val sc = new SparkContext(conf)

//    val data: List[String] = List("张三 张三", "李四 李四", "王五", "马六", "张三", "马六")

//    val lines = sc.parallelize(data)

    val lines = sc.textFile("C:\\Users\\Administrator.PC-20180301AKPZ\\Desktop\\spark_test.txt")

    val words = lines.flatMap{line => line.split(" ")}

    val pair = words.map{word => (word, 1)}

    val wordCounts = pair.reduceByKey{ _ + _ }

    wordCounts.foreach(wordCount => println(wordCount._1 + wordCount._2))

  }

}
