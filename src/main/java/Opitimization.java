package streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Opitimization {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Opitimization")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")  //当算子函数使用了外部比较大的对象时，启用Kryo序列化
                .set("spark.kryoserializer.buffer.mb","10")  //默认是2M大小的缓存，当自定义类比较大时调大这个参数
                .setMaster("local");
        //序列化对象默认会保存全限定类名，会耗费大量内存，注册自定义的类型会避免这种情况
        conf.registerKryoClasses(streaming.MyClass.class);
        JavaSparkContext sc = new JavaSparkContext(conf);

    }
}
