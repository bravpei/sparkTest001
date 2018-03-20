package com.moon.star

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.sql.SparkSession


class Sql (val appName:String,val url:String,val tableName:String,val user:String,val password:String,val sql:String){
  def commenceSql():Unit={
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val connectionProperties=new Properties()
    connectionProperties.put("user",user)
    connectionProperties.put("password",password)
    connectionProperties.put("diver","org.postgresql.Driver")
    tableName.split(",").foreach(x=>{
      spark.read.jdbc(url,x,connectionProperties).createOrReplaceTempView(x)
    })
    val result=spark.sql(sql)
    val time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
    result.rdd.saveAsTextFile("hdfs://172.18.130.100/liupei/test/results/"+time)
  }
}