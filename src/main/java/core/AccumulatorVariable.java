package core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

public class AccumulatorVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("core.AccumulatorVariable")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final Accumulator<Integer> acc = sc.accumulator(0);

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        rdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                acc.add(integer);
            }
        });
        System.out.println(acc.value());

        sc.stop();
    }
}
