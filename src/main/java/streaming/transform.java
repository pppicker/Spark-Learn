package streaming;

import com.google.common.base.Optional;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

public class transform {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("transform")
                .setMaster("local");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","192.168.52,92");
        Set<String> topics = new HashSet<String>();
        topics.add("test");
        //输入数据格式 “时间戳 姓名”
        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(
                ssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);

//        JavaDStream<String> lines = directStream.map(new Function<Tuple2<String, String>, String>() {
//            private static final long serialVersionUID = -6433187752517523148L;
//
//            @Override
//            public String call(Tuple2<String, String> v1) throws Exception {
//                return v1._2;
//            }
//        });
        //黑名单数据
        List<Tuple2<String, Boolean>> blacklist = new ArrayList<Tuple2<String, Boolean>>();
        final JavaPairRDD<String,Boolean> blacklistRDD = ssc.sc().parallelizePairs(blacklist);

        JavaPairDStream<String,String> pairs = directStream.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            private static final long serialVersionUID = 2130127135068892649L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
                return new Tuple2<String, String>(t._2.split(" ")[1], t._2);
            }
        });

        JavaDStream<String> filtered = pairs.transform(
                new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
                    private static final long serialVersionUID = 7446192587680204841L;

                    @Override
                    public JavaRDD<String> call(JavaPairRDD<String, String> pairRDD) throws Exception {
                        //joinedRDD 姓名，（日志，boolen）
                        JavaPairRDD<String,Tuple2<String,Optional<Boolean>>> joinedRDD = pairRDD.leftOuterJoin(blacklistRDD);

                        JavaPairRDD<String,Tuple2<String,Optional<Boolean>>> filteredRDD = joinedRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                            private static final long serialVersionUID = 3223178029126732223L;

                            @Override
                            public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) throws Exception {
                                if(v1._2._2.isPresent() && v1._2._2.get()) {
                                    return false;
                                }
                                return true;
                            }
                        });

                        JavaRDD<String> result = filteredRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                            private static final long serialVersionUID = 3291254397661701010L;

                            @Override
                            public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) throws Exception {
                                return v1._2._1;
                            }
                        });

                        return result;
                    }
                });

        ssc.start();
        ssc.awaitTermination();

    }
}
