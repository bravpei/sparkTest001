package com.moon.star

object ProcessDriver extends App{
  val process=List(Map("HdfsPath"->"hdfs://172.18.130.100/liupei/test/fiction.txt","nodeName"->"init","operation"->"","hasNext"->1),
    Map("nodeName"->"flatMap", "operation"->"_.split(\" \")","hasNext"->0))
  val pe=new ProcessEngine
  pe.execute(process,null)
}
