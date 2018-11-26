package cn.hadoop.sparkjava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @Author zz
 * @Date 2018/11/26 002615:24
 * @ClassName Main
 */
public class Main {
    public static void main(String[] args) {

    }

    /**
     * map算子
     * 1.创建SparkConf
     * 2.创建JavasparkContext
     * 3.构造集合
     * 4.并行化集合，创建初始RDD
     */
    public static void map(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        JavaRDD<Integer> multipleNumberRDD  = numbersRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return  integer * 2 ;
            }
        });
        multipleNumberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.close();
    }

    /**
     * filter算子
     */
    public static void filter(){
        SparkConf conf = new SparkConf().setMaster("lcoal").setAppName("filter");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        JavaRDD<Integer> evenNumberRDD = numberRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer / 2 == 0;
            }
        });
        evenNumberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.close();
    }

    /**
     * Spark 中 map函数会对每一条输入进行指定的操作，然后为每一条输入返回一个对象；
     * 而flatMap函数则是两个操作的集合——正是“先映射后扁平化”：
     * 操作1：同map函数一样：对每一条输入进行指定的操作，然后为每一条输入返回一个对象
    * 操作2：最后将所有对象合并为一个对象
     */
    public static void flatMap(){
        SparkConf conf = new SparkConf().setAppName("flatMap").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> lineList = Arrays.asList("hello java","hello scala","hello world");
        JavaRDD<String> lines = sc.parallelize(lineList);
        lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return (Iterator<String>) Arrays.asList(s.split(" "));
            }
        });
        sc.close();
    }

    /**
     * groupByKey算子
     */
    public static void groupByKey(){
        SparkConf conf = new SparkConf().setAppName("groupByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("math", 80),
                new Tuple2<String, Integer>("Chinese", 90),
                new Tuple2<String, Integer>("math", 97),
                new Tuple2<String, Integer>("Chinese", 89));
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);
        JavaPairRDD<String, Iterable<Integer>> groupedScores = scores.groupByKey();
        groupedScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println("class: "+stringIterableTuple2._1);
                stringIterableTuple2._2.forEach(System.out::println);
            }
        });
        sc.close();
    }
    /**
     * reduceByKey算子
     */
    public static void reduceByKey(){
        SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("math", 80),
                new Tuple2<String, Integer>("Chinese", 90),
                new Tuple2<String, Integer>("math", 97),
                new Tuple2<String, Integer>("Chinese", 89));
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);
        JavaPairRDD<String, Integer> totalScore = scores.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });
        totalScore.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + "." + stringIntegerTuple2._2);
            }
        });
        sc.close();
    }
    /**
     * sortByKey算子
     */
    public static void sortByKey(){
        SparkConf conf = new SparkConf().setAppName("sortByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> scoreList = Arrays.asList(
                new Tuple2<Integer, String>(78, "marry"),
                new Tuple2<Integer, String>(89, "tom"),
                new Tuple2<Integer, String>(72, "jack"),
                new Tuple2<Integer, String>(86, "leo"));
        JavaPairRDD<Integer, String> scores = sc.parallelizePairs(scoreList);
        JavaPairRDD<Integer, String> sortedScores = scores.sortByKey();
        sortedScores.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println( integerStringTuple2._1+"."+ integerStringTuple2._2);
            }
        });
        sc.close();
    }
    /**
     *join算子
     *join算子用于关联两个RDD，join以后，会根据key进行join，并返回JavaPairRDD。
     * JavaPairRDD的第一个泛型类型是之前两个JavaPairRDD的key类型，因为通过key进行join的。
     * 第二个泛型类型，是Tuple2<v1, v2>的类型，Tuple2的两个泛型分别为原始RDD的value的类型
     */
    public static void join(){
        SparkConf conf = new SparkConf().setAppName("join").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "tom"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "marry"),
                new Tuple2<Integer, String>(4, "leo"));

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 78),
                new Tuple2<Integer, Integer>(2, 87),
                new Tuple2<Integer, Integer>(3, 89),
                new Tuple2<Integer, Integer>(4, 98));
        //并行化两个RDD
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);;
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);
        //使用join算子关联两个RDD
        //join以后，会根据key进行join，并返回JavaPairRDD
        //JavaPairRDD的第一个泛型类型，之前两个JavaPairRDD的key类型，因为通过key进行join的
        //第二个泛型类型，是Tuple2<v1, v2>的类型，Tuple2的两个泛型分别为原始RDD的value的类型
        JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores = students.join(scores);
        //打印
        studentScores.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t)
                    throws Exception {
                System.out.println("student id:" + t._1);
                System.out.println("student name:" + t._2._1);
                System.out.println("student score:" + t._2._2);
                System.out.println("==========================");
            }
        });
        sc.close();
    }
}
