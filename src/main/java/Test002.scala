import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel


object Test002 {
  def main(args: Array[String]): Unit = {
/*    val conf = new SparkConf().setAppName("Test002")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint("checkpoint")
    val para = Map("metadata.broker.list" -> "172.18.130.101:6667","serializer.clas"->"kafka.serializer.StringEncoder")
    val topic=Set("test")
    val lines = KafkaUtils.createDirectStream(ssc, para, topic).map(x=>x._2)
    ssc.start()
    ssc.awaitTermination()*/
  }
  }
