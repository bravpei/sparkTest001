import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

object Test004 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_COMMON_LIB_NATIVE_DIR", "/usr/hdp/2.6.3.0-235/hadoop/lib/native")
    System.setProperty("HADOOP_USER_NAME","hdfs")
    val date = new Date()
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val before = sdf.format(date).toLong
    val conf=new SparkConf().setAppName("test004").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("WARN")
    val testfile=sc.textFile("hdfs://172.18.130.100/user/hdfs/sqoop_table/*")
    val str=testfile.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2>20).take(20).foreach(println)
    val date1 = new Date()
    val after = sdf.format(date1).toLong
    println(f"cost:${after-before}")
  }
}
