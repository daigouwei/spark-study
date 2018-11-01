package com.gg.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author daigouwei
 * @date 2018/10/19
 */
public class JavaPairRDDExample {
    public static void main(String[] args) {
    }

    private JavaSparkContext sc;

    private void init() {
        this.sc = new JavaSparkContext(new SparkConf().setAppName("JavaPairRDD demo"));
    }

    /**
     * reduceByKey对所有的单词进行计数
     */
    private void testReduceByKey() {
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("1 one", "2 two", "3 three", "4 four"));
        JavaPairRDD<String, Integer> pairLines = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(line -> new Tuple2<String, Integer>(line, 1)).reduceByKey((sum1, sum2) -> sum1 + sum2);
        //打印
        pairLines.collect().forEach(tuple -> System.out.println(tuple._1() + ":" + tuple._2()));
    }

    /**
     * foldByKey对所有的单次进行计数，但是可以赋初始值zeroValue
     */
    private void testFoldByKey() {
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("1 one", "2 two", "3 three", "4 four"));
        JavaPairRDD<String, Integer> pairLines = lines.flatMapToPair(
                line -> Arrays.stream(line.split(" ")).map(word -> new Tuple2<String, Integer>(word, 1))
                        .collect(Collectors.toList()).iterator()).foldByKey(10, (sum1, sum2) -> sum1 + sum2);
        //打印
        pairLines.collect().forEach(tuple -> System.out.println(tuple._1() + ":" + tuple._2()));
    }

    /**
     * combineByKey求平均值
     */
    private void testCombineByKey() {
        JavaRDD<String> lines =
                sc.parallelize(Arrays.asList("gw 90", "gw 91", "mll 80", "wzx 78", "gw 79", "mll 89", " wzx 20"));
        JavaPairRDD<String, Tuple2<Integer, Integer>> pairLines = lines.mapToPair(
                line -> new Tuple2<String, Integer>(line.split(" ")[0], Integer.valueOf(line.split(" ")[1])))
                .combineByKey(score -> new Tuple2<Integer, Integer>(score, 1),
                        (scoreTuple, score) -> new Tuple2<Integer, Integer>(scoreTuple._1() + score,
                                scoreTuple._2() + 1),
                        (scoreTuple1, scoreTuple2) -> new Tuple2<Integer, Integer>(scoreTuple1._1() + scoreTuple2._1(),
                                scoreTuple1._2() + scoreTuple2._2()));
        //打印
        pairLines.collect().forEach(tuple -> System.out.println(tuple._1() + ": " + (tuple._2()._1() / tuple._2._2)));
    }

    /**
     * groupByKey对数据按照key进行分组
     */
    private void testGroupByKey() {
        JavaPairRDD<String, Integer> pairLines = sc.parallelizePairs(
                Arrays.asList(new Tuple2<String, Integer>("gw", 90), new Tuple2<String, Integer>("gw", 80),
                        new Tuple2<String, Integer>("mll", 75)));
        JavaPairRDD<String, Iterable<Integer>> pairLines2 = pairLines.groupByKey();
        //打印
        pairLines2.collect().forEach(tuple -> System.out.println(
                tuple._1() + ":" + StreamSupport.stream(tuple._2().spliterator(), false).collect(Collectors.toList())));
    }

    /**
     * sortByKey对数据按照key排序，可以自定义java排序器
     */
    private void testSortByKey() {
        JavaPairRDD<Integer, Integer> pairLines = sc.parallelizePairs(
                Arrays.asList(new Tuple2<Integer, Integer>(1, 3), new Tuple2<Integer, Integer>(3, 3),
                        new Tuple2<Integer, Integer>(1, 3), new Tuple2<Integer, Integer>(2, 3)));
        JavaPairRDD<Integer, Integer> pairLines2 = pairLines.sortByKey(Integer::compareTo);
        //打印
        pairLines.collect().forEach(tuple -> System.out.println(tuple._1() + ":" + tuple._2()));
    }

}
