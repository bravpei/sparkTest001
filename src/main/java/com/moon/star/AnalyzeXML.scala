package com.moon.star

import java.net.URL

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory

import scala.xml.XML

object AnalyzeXML {
  def main(args: Array[String]): Unit = {
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
    val xml=XML.load(new URL(args(0)))
    val values=xml \ "property" \ "value"
    val list=values.map(_.text).toList
    val sqlTest=new Sql(list(0),list(1),list(2),list(3),list(4),list(5))
    sqlTest.commenceSql()
  }
}