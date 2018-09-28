import org.apache.spark.sql.SparkSession

object PartsDelay {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("partsDelay").master("local[*]").getOrCreate()
    val df = spark.read.format("jdbc").option("url", "jdbc:postgresql://172.18.130.101:5432/postgres").option("dbtable", "dw.parts_delay").option("user", "gpadmin").option("password", "gpadmin").option("diver", "org.postgresql.Driver").load
    val a=df.repartition(0)
    println(df.count())
    println(a.count())
  }
  }
