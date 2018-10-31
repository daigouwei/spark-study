package com.gg.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 *
 * @author daigouwei
 * @date 2018/10/19
 */
public class JavaRDDExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaRDD demo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("one", "two", "three"));
        long sum = lines.count();
        System.out.println(sum);
    }
}
