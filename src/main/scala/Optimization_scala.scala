import org.apache.spark.{SparkConf, SparkContext}
import streaming.Myclass

object Optimization_scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Optimization")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //当算子函数使用了外部比较大的对象时，启用Kryo序列化
      .set("spark.kryoserializer.buffer.mb", "10") //默认是2M大小的缓存，当自定义类比较大时调大这个参数
      .set("spark.default.parallelism", "5") //spark官方推荐并行度设置为每个cpu核心分配2-3个task任务 即 excutore * excutor cores * 2|3
      .setMaster("local")

    //序列化对象默认会保存全限定类名，会耗费大量内存，注册自定义的类型会避免这种情况
    conf.registerKryoClasses(Array(classOf[Myclass]))
    val sc = new SparkContext(conf)
    /**在算子函数中，需要优化处理的数据结构
      * 1.优先使用数组以及字符串，而不是集合类
      * 2.对于多层嵌套的对象使用json串进行替代
      * 3.尽量使用Int替代String
      */

    /**对于多次操作的RDD，进行持久化和checkpoint操作
      * 1.如果RDD的数据量比较大，要启用序列化的持久化并启用kryo序列化，因为内存中数据量比较大的话可能导致频繁GC，从而导致task工作线程频繁停止
      * 2.如果通过SparkUI观察到频繁进行GC，可以设置SparkConf().set("spark.storage.memoryFraction","0.5")适当调小给RDD缓存分配的内存比例
      */
  }

}