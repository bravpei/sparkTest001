package test;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class JavaSparktest {
public static void main(String[] args){
    SparkConf conf=new SparkConf().setAppName("test").setMaster("local[*]");
    JavaSparkContext sc=new JavaSparkContext(conf);
    List<Integer> data= Arrays.asList(1,2,3,4);
    JavaRDD<Integer> distData=sc.parallelize(data);
    System.out.println(distData.collect());
}

}
