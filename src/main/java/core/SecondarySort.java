package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 二次排序
 * 1.实现自定义key，实现Ordered接口和Serializable接口，在key中实现自己对多个字段的排序算法。
 * 2.将包含数据的RDD映射为key为自定义key,value为原始数据的JavaPairRDD。
 * 3.使用sortByKey算子按照自定义key进行排序。
 * 4.再次映射，剔除自定义key,保留需要的数据。
 */

public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("secondaysort")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List data = Arrays.asList(
                new Tuple2<Integer, Integer>(1,1),
                new Tuple2<Integer, Integer>(1,2),
                new Tuple2<Integer, Integer>(1,3),
                new Tuple2<Integer, Integer>(2,1),
                new Tuple2<Integer, Integer>(2,2),
                new Tuple2<Integer, Integer>(2,3),
                new Tuple2<Integer, Integer>(3,1),
                new Tuple2<Integer, Integer>(3,2),
                new Tuple2<Integer, Integer>(3,3)
        );

        JavaPairRDD<Integer, Integer> lines = sc.parallelizePairs(data);

        JavaPairRDD<SecondarySortKey,Tuple2<Integer, Integer>> key_data = lines.mapToPair(new PairFunction<Tuple2<Integer,Integer>, SecondarySortKey, Tuple2<Integer, Integer>>() {
            private static final long serialVersionUID = 2848958408795070711L;

            @Override
            public Tuple2<SecondarySortKey, Tuple2<Integer, Integer>> call(Tuple2<Integer, Integer> t) throws Exception {
                SecondarySortKey key = new SecondarySortKey(t._1,t._2);
                return new Tuple2<SecondarySortKey, Tuple2<Integer, Integer>>(key,t);
            }
        });

        JavaPairRDD<SecondarySortKey,Tuple2<Integer, Integer>> sorted_key_data = key_data.sortByKey(false);
        sorted_key_data.foreach(new VoidFunction<Tuple2<SecondarySortKey, Tuple2<Integer, Integer>>>() {
            private static final long serialVersionUID = -8408768582820076795L;

            @Override
            public void call(Tuple2<SecondarySortKey, Tuple2<Integer, Integer>> secondarySortKeyTuple2Tuple2) throws Exception {
                System.out.println(secondarySortKeyTuple2Tuple2);
            }
        });


        sc.stop();
    }

}
