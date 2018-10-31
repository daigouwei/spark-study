package com.gg.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author daigouwei
 * @date 2018/10/19
 */
public class JavaPairRDDExample {
    public static void main(String[] args) {
        /*
        * combineByKey
        * 可以让用户返回与输入数据的类型不同的返回值
        *
        **/
    }

    private JavaSparkContext sc;
    private JavaRDD<String> lines;
    private JavaPairRDD<String, String> pairLines;

    private void init() {
        this.sc = new JavaSparkContext(new SparkConf().setAppName("JavaPairRDD demo"));
        this.lines = sc.parallelize(Arrays.asList("1 one", "2 two", "3 three", "4 four"));
        this.pairLines = lines.mapToPair(line -> new Tuple2<String, String>(line.split(" ")[0], line));
    }

    /**
     * reduceByKey对所有的单词进行计数
     *
     * @param lines
     */
    private void testReduceByKey(JavaRDD<String> lines) {
        JavaPairRDD<String, Integer> pairLines = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(line -> new Tuple2<String, Integer>(line, 1)).reduceByKey((sum1, sum2) -> sum1 + sum2);
        //打印
        pairLines.collect().forEach(tuple -> System.out.println(tuple._1() + ":" + tuple._2()));
    }

    /**
     * combineByKey可以让用户返回与输入数据的类型不同的返回值
     *
     * @param lines
     */
    private void testCombineByKey(JavaRDD<String> lines) {

    }

}
