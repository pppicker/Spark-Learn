package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Action {
    public static void main(String[] args) {
//        reduce();
//        collect();
//        count();
//        take();
//        savaAsTextFile();
        countByKey();
    }

    private static void reduce(){
        SparkConf conf = new SparkConf()
                .setAppName("reduce")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        Integer sum = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 8949096025348787196L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(sum);
        sc.stop();

    }

    private static void collect(){
        SparkConf conf = new SparkConf()
                .setAppName("collect")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
            private static final long serialVersionUID = -2481190131258650424L;

            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        List<Integer> l = result.collect();
        System.out.println(l);
        sc.stop();

    }

    private static void count(){
        SparkConf conf = new SparkConf()
                .setAppName("count")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        long count = rdd.count();

        System.out.println(count);
        sc.stop();

    }

    private static void take(){
        SparkConf conf = new SparkConf()
                .setAppName("take")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        List<Integer> take = rdd.take(3);

        System.out.println(take);
        sc.stop();

    }

    private static void savaAsTextFile(){
        SparkConf conf = new SparkConf()
                .setAppName("savaAsTextFile")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        //这里由于本地没有winutile包，与这个相关的一个文件系统权限判定BUG会导致保存到windows失败，linux上不受影响。
        rdd.saveAsTextFile("C://Users//Administrator.PC-20180301AKPZ//Desktop//jdk//spark_result.txt");
        sc.stop();
    }

    private static void countByKey(){
        SparkConf conf = new SparkConf()
                .setAppName("countByKey")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String,String>> data = Arrays.asList(
                new Tuple2<String, String>("class1", "张三"),
                new Tuple2<String, String>("class1", "李四"),
                new Tuple2<String, String>("class1", "王五"),
                new Tuple2<String, String>("class1", "马六"),
                new Tuple2<String, String>("class2", "赵琦"),
                new Tuple2<String, String>("class2", "琳琳"),
                new Tuple2<String, String>("class2", "丽丽")
        );
        JavaPairRDD<String,String> rdd = sc.parallelizePairs(data);
        Map<String, Object> result = rdd.countByKey();

        for(Map.Entry<String, Object> studentCount : result.entrySet()) {
            System.out.println(studentCount.getKey() + " " + studentCount.getValue());
        }
        sc.stop();
    }

    public static class TopN {
        public static void main(String[] args) {
            SparkConf conf = new SparkConf()
                    .setAppName("collect")
                    .setMaster("local");
            JavaSparkContext sc = new JavaSparkContext(conf);

    //        List<Integer> data1 = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
    //
    //        JavaRDD<Integer> rdd = sc.parallelize(data1);
    //
    //        JavaPairRDD<Integer, Integer> pair = rdd.mapToPair(new PairFunction<Integer, Integer, Integer>() {
    //            private static final long serialVersionUID = 8898696569774903339L;
    //
    //            @Override
    //            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
    //                return new Tuple2<Integer, Integer>(integer,integer);
    //            }
    //        });
    //        JavaPairRDD<Integer, Integer> sorted_pair = pair.sortByKey(false);
    //        JavaRDD<Integer> sorted_result = sorted_pair.map(new Function<Tuple2<Integer, Integer>, Integer>() {
    //            private static final long serialVersionUID = -9173371592558173661L;
    //
    //            @Override
    //            public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
    //                return v1._2;
    //            }
    //        });
    //        List top3 = sorted_result.take(3);
    //        System.out.println(top3);

            List<Tuple2<String,Integer>> data2 = Arrays.asList(
                    new Tuple2<String, Integer>("class1", 88),
                    new Tuple2<String, Integer>("class1", 23),
                    new Tuple2<String, Integer>("class1", 33),
                    new Tuple2<String, Integer>("class1", 44),
                    new Tuple2<String, Integer>("class1", 77),
                    new Tuple2<String, Integer>("class2", 66),
                    new Tuple2<String, Integer>("class2", 11),
                    new Tuple2<String, Integer>("class2", 61),
                    new Tuple2<String, Integer>("class2", 55),
                    new Tuple2<String, Integer>("class2", 75)
            );
            JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);
            JavaPairRDD<String, Iterable<Integer>> group_data = rdd2.groupByKey();

            //在mapToPair算子中实现分组取topN的算法
            JavaPairRDD<String, Iterable<Integer>> group_top3 = group_data.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
                private static final long serialVersionUID = -4735875111086850579L;

                @Override
                public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> group_data) throws Exception {
                    String className = group_data._1;
                    Integer[] top3 = new Integer[3];
                    Iterator<Integer> scores = group_data._2.iterator();
                    while(scores.hasNext()) {
                        Integer score = scores.next();
                        for(int i = 0; i < 3; i++) {
                            if(top3[i] == null) {
                                top3[i] = score;
                                break;
                            } else if(score > top3[i]) {
                                for(int j = 2; j > i ; j--) {
                                    top3[j] =top3[j-1];
                                }
                                top3[i] = score;
                                break;
                            }
                        }
                    }
                    return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(top3));
                }
            });
            group_top3.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
                private static final long serialVersionUID = -2170623007791974882L;

                @Override
                public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {

                    String className = t._1;
                    System.out.println(className);
                    Iterator<Integer> ite = t._2.iterator();
                    while (ite.hasNext()) {
                        System.out.println(ite.next());
                    }
                    System.out.println("======================");
                }
            });



            sc.stop();


        }
    }
}
