package streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object transform_scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("transform")
      .setMaster("local")
    val ssc = new StreamingContext(conf, Seconds(5))

    val blacklist = Array(("小明", true))
    val blacklistRDD = ssc.sparkContext.parallelize(blacklist)

    val brokers = "192.129.23.12"
    val topics = "test".split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val directkafka_dstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topics)

    val pair = directkafka_dstream.map(x => (x._2.split(" ")(1), x._2))
    val filtered = pair.transform(pair => {
      val joinedRDD = pair.leftOuterJoin(blacklistRDD)
      val filteredRDD = joinedRDD.filter(x => {
        if(x._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
      })
      val result = filteredRDD.map(x => x._2._1)
      result
    })
    filtered.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
