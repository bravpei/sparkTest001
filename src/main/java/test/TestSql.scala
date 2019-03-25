package test

import org.apache.spark.sql.SparkSession

object TestSql {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("test002").master("local[*]")getOrCreate()
    val df=spark.read.schema("birth Date ,name string,age int").option("delimiter"," ").csv("file:///home/liupei/test/test.txt")
    df.show
    df.printSchema()
  }
}
