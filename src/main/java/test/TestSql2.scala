package test

import org.apache.spark.sql.SparkSession

object TestSql2 {
  //求每天温度的平均值
  def main(args: Array[String]): Unit = {
    //原始数据
/*    val spark = SparkSession.builder().appName("test002").master("local[*]") getOrCreate()
    val df = spark.read.schema("date Date ,hour int,tem double").option("delimiter", " ").csv("file:///home/liupei/test/59431format.txt")
    df.createOrReplaceTempView("test")
    spark.sql("select date,avg(tem)  from test  group by date order by date").show*/


    //拆分后的数据
    val spark = SparkSession.builder().appName("test002").master("local[*]") getOrCreate()
    val df = spark.read.schema("date Date ,t0 double,t1 double,t2 double,t3 double,t4 double,t5 double," +
      "t6 double,t7 double,t8 double,t9 double,t10 double,t11 double,t12 double,t13 double," +
      "t14 double,t15 double,t16 double,t17 double,t18 double,t19 double,t20 double,t21 double,t22 double,t23 double").option("delimiter", " ").csv("file:///home/liupei/test/column3")
    df.createOrReplaceTempView("test")
    spark.sql("select date,(t0+t1+t2+t3+t4+t5+t6+t7+t8+t9+t10+t11+t12+t13+t14+t15+t16+t17+t18+t19+t20+t21+t22+t23)/24 avg from test  order by date")
        .show
  }
}
