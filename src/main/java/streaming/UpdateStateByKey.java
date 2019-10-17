package streaming;

import com.google.common.base.Optional;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

public class UpdateStateByKey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("streaming.UpdateStateByKey")
                .setMaster("local");
        JavaStreamingContext jssc = new JavaStreamingContext(
                conf, Durations.seconds(5));
        //UpdateStateByKey必须设置检查点
        jssc.checkpoint("hdfs://192.168.1.105:9000/streaming_checkpoint");


        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","192.168.52,92");
        Set<String> topics = new HashSet<String>();
        topics.add("test");
        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);

        JavaDStream<String> lines = directStream.map(new Function<Tuple2<String, String>, String>() {
            private static final long serialVersionUID = -6433187752517523148L;

            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._2;
            }
        });
        JavaDStream<String> set = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = -4947449374930087935L;

            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairDStream<String, Integer> pair = set.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 4163696890429522168L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCount = pair.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            private static final long serialVersionUID = 5622761447351946616L;

            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                Integer newValue = 0;
                if(state.isPresent()){
                    newValue = state.get();
                }
                for(Integer value : values){
                    newValue += value;
                }
                return Optional.of(newValue);
            }
        });

        wordCount.print();

    }
}
