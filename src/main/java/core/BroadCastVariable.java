package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

public class BroadCastVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("core.BroadCastVariable")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final int factor = 3;
        final Broadcast<Integer> factorBroadcast = sc.broadcast(factor);

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        JavaRDD<Integer> result = rdd.map(new Function<Integer,Integer>() {
            private static final long serialVersionUID = 9212222173082308796L;

            @Override
            public Integer call(Integer v1) throws Exception {
                int factor = factorBroadcast.value();
                return v1 * factor;
            }
        });
        result.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = 2048546598969432444L;

            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.stop();
    }
}
