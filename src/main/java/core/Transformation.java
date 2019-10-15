package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Transformation {
    public static void main(String[] args) {
        map();
        filter();
        flatmap();
        groupByKey();
        reduceByKey();
        sortByKey();
        join();
        cogroup();
    }




    private static void map(){
        SparkConf conf = new SparkConf()
                .setAppName("map")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> number = sc.parallelize(data);

        JavaRDD<Integer> doubleNum = number.map(new Function<Integer, Integer>() {
            private static final long serialVersionUID = 1627667162779601085L;

            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        doubleNum.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = 5128809498441321510L;

            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.stop();
    }
    private static void filter(){

        SparkConf conf = new SparkConf()
                .setAppName("map")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> number = sc.parallelize(data);

        JavaRDD<Integer> result = number.filter(new Function<Integer, Boolean>() {
            private static final long serialVersionUID = 1179586830093834101L;

            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1%2 == 0;
            }
        });
        result.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = 6569231029140075035L;

            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.stop();

    }

    private static void flatmap(){
        SparkConf conf = new SparkConf()
                .setAppName("flatmap")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> data = Arrays.asList("hello world! today is good day!","java scala spark hadoop streaming kafka etl python spider");
        JavaRDD<String> rdd = sc.parallelize(data);

        JavaRDD<String> result = rdd.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = -2481118309671012937L;

            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        result.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = 5158230981739862177L;

            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        sc.stop();
    }

    private static void groupByKey(){
        SparkConf conf = new SparkConf()
                .setAppName("groupByKey")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String,Integer>> data = Arrays.asList(
                new Tuple2<String, Integer>("class1", 88),
                new Tuple2<String, Integer>("class1", 23),
                new Tuple2<String, Integer>("class1", 33),
                new Tuple2<String, Integer>("class1", 44),
                new Tuple2<String, Integer>("class2", 77),
                new Tuple2<String, Integer>("class2", 66),
                new Tuple2<String, Integer>("class2", 11)
        );
        JavaPairRDD<String,Integer> rdd = sc.parallelizePairs(data);

        JavaPairRDD<String,Iterable<Integer>> result = rdd.groupByKey();

        result.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            private static final long serialVersionUID = -606341932882786223L;

            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println(t._1);
                Iterator<Integer> ite = t._2.iterator();
                while(ite.hasNext()) {
                    System.out.println(ite.next());
                }
                System.out.println("======================");
            }
        });
        sc.stop();
    }


    private static void reduceByKey(){
        SparkConf conf = new SparkConf()
                .setAppName("reduceByKey")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String,Integer>> data = Arrays.asList(
                new Tuple2<String, Integer>("class1", 88),
                new Tuple2<String, Integer>("class1", 23),
                new Tuple2<String, Integer>("class1", 33),
                new Tuple2<String, Integer>("class1", 44),
                new Tuple2<String, Integer>("class2", 77),
                new Tuple2<String, Integer>("class2", 66),
                new Tuple2<String, Integer>("class2", 11)
        );
        JavaPairRDD<String,Integer> rdd = sc.parallelizePairs(data);

        JavaPairRDD<String,Integer> result = rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -5761748907000364667L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = -606341932882786223L;

            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " " + t._2);
            }
        });
        sc.stop();
    }

    private static void sortByKey(){
        SparkConf conf = new SparkConf()
                .setAppName("sortByKey")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> data = Arrays.asList(
                new Tuple2<String, Integer>("张三", 42),
                new Tuple2<String, Integer>("李四", 35),
                new Tuple2<String, Integer>("王五", 88),
                new Tuple2<String, Integer>("马六", 74),
                new Tuple2<String, Integer>("赵琦", 24)
        );
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);

        //为什么JavaPairRDD<String,Integer>类型的rdd调用map之后，返回的rdd类型变成了JavaPairRDD<Tuple2<>>,而不是期望的JavaPairRDD<Integer,String>
        //要如何实现才能达到期望的结果？
//        JavaPairRDD<Tuple2<Integer, String>> revertRdd = rdd.map(new Function<Tuple2<String,Integer>, Tuple2<Integer,String>>() {
//
//            private static final long serialVersionUID = -2690963863245656921L;
//            @Override
//            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
//                return new Tuple2<Integer, String>(t._2,t._1);
//            }
//        });
        JavaPairRDD<Integer, String> revertRdd = rdd.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            private static final long serialVersionUID = -2426798651748378926L;

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer, String>(t._2, t._1);
            }
        });

        JavaPairRDD<Integer,String> result = revertRdd.sortByKey(false);

        result.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> t) throws Exception {
                System.out.println(t._1 + " " + t._2);
            }
        });
        sc.stop();
    }

    private static void join(){
        SparkConf conf = new SparkConf()
                .setAppName("join")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> data1 = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 45),
                new Tuple2<Integer, Integer>(2, 75),
                new Tuple2<Integer, Integer>(3, 64),
                new Tuple2<Integer, Integer>(4, 90),
                new Tuple2<Integer, Integer>(1, 55)
        );
        List<Tuple2<Integer, String>> data2 = Arrays.asList(
                new Tuple2<Integer, String>(1, "张三"),
                new Tuple2<Integer, String>(2, "李四"),
                new Tuple2<Integer, String>(3, "王五"),
                new Tuple2<Integer, String>(4, "马六")
        );
        JavaPairRDD<Integer, Integer> rdd1 = sc.parallelizePairs(data1);
        JavaPairRDD<Integer, String> rdd2 = sc.parallelizePairs(data2);

        JavaPairRDD<Integer,Tuple2<Integer,String>> result = rdd1.join(rdd2);


        result.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Integer, String>>>() {
            private static final long serialVersionUID = -3554076469720462288L;

            @Override
            public void call(Tuple2<Integer, Tuple2<Integer, String>> t) throws Exception {
                System.out.println(t._1 + " " + t._2);
            }
        });
        sc.stop();
    }
    private static void cogroup(){
        SparkConf conf = new SparkConf()
                .setAppName("cogroup")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> data1 = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 45),
                new Tuple2<Integer, Integer>(2, 75),
                new Tuple2<Integer, Integer>(3, 64),
                new Tuple2<Integer, Integer>(4, 90),
                new Tuple2<Integer, Integer>(1, 55)
        );
        List<Tuple2<Integer, String>> data2 = Arrays.asList(
                new Tuple2<Integer, String>(1, "张三"),
                new Tuple2<Integer, String>(2, "李四"),
                new Tuple2<Integer, String>(3, "王五"),
                new Tuple2<Integer, String>(4, "马六")
        );
        JavaPairRDD<Integer, Integer> rdd1 = sc.parallelizePairs(data1);
        JavaPairRDD<Integer, String> rdd2 = sc.parallelizePairs(data2);

        JavaPairRDD<Integer,Tuple2<Iterable<Integer>,Iterable<String>>> result = rdd1.cogroup(rdd2);


        result.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<String>>>>() {
            private static final long serialVersionUID = -721049376255228300L;

            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<String>>> t) throws Exception {
                System.out.println(t._1 + "  " +t._2);
            }
        });
        sc.stop();
    }

}
