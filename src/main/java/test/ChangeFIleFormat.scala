package test

import org.apache.spark.{SparkConf, SparkContext}

object ChangeFIleFormat {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("test").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val rdd=sc.textFile("file:///home/liupei/test/59431.txt")
    rdd.map(_.replace("\t"," ")).saveAsTextFile("file:///home/liupei/test/newfile")
  }
}
