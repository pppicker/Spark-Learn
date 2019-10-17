package streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKey_scala {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("UpdateStateByKey")
      .setMaster("local")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint(".")

//    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
//      val currentCount = values.sum
//      val previousCount = state.getOrElse(0)
//      Some(currentCount + previousCount)
//    }

    val brokers = "192.129.23.12"
    val topics = "test".split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val directkafka_dstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topics)

    val wordCount = directkafka_dstream.map(_._2).flatMap(_.split(" ")).map(x => (x, 1))
    val stateDstream = wordCount.updateStateByKey[Int](
      (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Option(currentCount + previousCount)
    }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
