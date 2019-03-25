package test

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object TestSql3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate
    val df = spark.read.schema("date Date ,hour int,column3 double,column4 double,column5 double," +
      "column6 double,column7 double,column8 double,column9 double,column10 double,column11 double").option("delimiter", " ").csv("file:///home/liupei/test/59431format.txt")
    df.createOrReplaceTempView("test2")
    //df.show
    spark.sql("select * from (select date,hour,column3 from test2) pivot(sum(column3) for hour in('0','1','2','3','4','5','6','7'," +
      "'8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23')) order by date").write.option("delimiter", " ").csv("file:///home/liupei/test/column3")
  }
}
