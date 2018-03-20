package com.moon.star

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class ProcessEngine {
  def execute(process:List[Map[String,Any]],rdd:RDD[String]): Unit ={
    val nodeMap=process.head
    nodeMap.getOrElse("nodeName",0) match {
      case "init"=>
        val r=init(nodeMap,rdd)
        if(nodeMap("hasNext")==1){
          execute(process.tail,r)
        }else println("Completed!")
      case "flatMap"=>
        val r=flatMap(nodeMap,rdd)
        if(nodeMap("hasNext")==1){
          execute(process.tail,r)
        }else println("Completed!")
    }

  }
  def init(nodeMap:Map[String,Any],rdd:RDD[String]): RDD[String] ={
    val conf=new SparkConf().setAppName("test001").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.textFile(nodeMap("HdfsPath").toString)
  }
  def flatMap(nodeMap:Map[String,Any],rdd:RDD[String]): RDD[String]={
    rdd.flatMap(nodeMap("operation").asInstanceOf[String => TraversableOnce[String]])
  }

}
