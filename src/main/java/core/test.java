package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class test {
    public static void main(String[] args) {
//        SparkConf conf = new SparkConf()
//                .setAppName("core.test")
//                .setMaster("local[2]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        List<Integer> data = Arrays.asList(1,2,3,4,5);
//        JavaRDD<Integer> num = sc.parallelize(data);
//
//        List<String> s = Arrays.asList("hello world");

        String ss = "hello world";
        System.out.println(ss.split(" "));
    }
}
