package com.bigdata.streaming.spark.java.core.inputsource;

import com.bigdata.streaming.common.CommKey;
import com.bigdata.streaming.spark.java.SparkDevHelper;
import com.google.common.collect.ImmutableList;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;
import scala.Tuple2;

import java.util.*;

public class MyDIYInputRDD extends SparkDevHelper {

    public static class MyFlatMap implements FlatMapFunction<String,String> {
        @Override
        public Iterator<String> call(String input) throws Exception {
            Iterator<String> iterator = Arrays.asList(input.split(CommKey.EMPTY_STRING)).iterator();
            return iterator;
        }
    }

    public static class MyPairFunc implements PairFunction<String,String,Integer> {
        @Override
        public Tuple2<String, Integer> call(String s) throws Exception {
            return new Tuple2<>(s,1);
        }
    }

    public static class MyFunc2 implements Function2<Integer,Integer,Integer> {
        @Override
        public Integer call(Integer v1, Integer v2) throws Exception {
            return v1 + v2;
        }
    }

    public static class MyMapPartIndexFunc2 implements Function2<Integer,Iterator<Tuple2<String, Integer>>,Iterator<String>> {
        @Override
        public Iterator<String> call(Integer index, Iterator<Tuple2<String, Integer>> it) throws Exception {
            List<String> list = new ArrayList<>();
            it.forEachRemaining(t->{
                list.add(t.toString());
            });
            if(!list.isEmpty()){
                System.err.println("分区_"+index+": "+list.toString());
                return list.iterator();
            }else {
                return  Collections.emptyIterator();
            }
        }
    }

    @Test
    public void testRDD(){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass().getSimpleName());
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> line = jsc.parallelize(ImmutableList.of(
                "Spark Kafka Streaming BigData Hadoop",
                "Java Scala Object Class Spark JVM",
                "Spark Java Bigdata Scala Hadoop Flink BigData",
                "Streaming BigData Hadoop Object Class Spark JVM",
                "Spark Java Bigdata Scala Hadoop Flink",
                "Java Scala Object Class  Java Bigdata Scala Hadoop Flink BigData",
                "Streaming BigData Hadoop Flink"
        ),2);

        JavaRDD<String> words = line.flatMap(new MyFlatMap());

        JavaPairRDD<String, Integer> wordsWithCount = words.mapToPair(new MyPairFunc());

        JavaPairRDD<String, Integer> wordsCount = wordsWithCount.reduceByKey(new MyFunc2());

        JavaRDD<String> result = wordsCount.mapPartitionsWithIndex(new MyMapPartIndexFunc2(), false);

        result.collect().forEach(r->{
            System.out.println(r);
        });
    }


    @Test
    public void testQueueDStream(){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass().getSimpleName());
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(3000));

        JavaSparkContext jsc = jssc.sparkContext();
        jsc.setLogLevel(Level.WARN.toString());
        Queue<JavaRDD<String>> queue = new LinkedList<>();
        int partitionNum = 2;

        List<String> lines = ImmutableList.of(
                "Spark Kafka Streaming BigData Hadoop",
                "Streaming BigData Hadoop Object Class Spark JVM",
                "Spark Kafka Streaming Class Spark BigData Hadoop",
                "Streaming BigData Hadoop Object Class Spark JVM",
                "Java Scala Object Class Spark Class Spark JVM",
                "Java Scala BigData Hadoop Object Class  JVM",
                "Spark Java Bigdata Scala Hadoop Flink"
        );

        queue.add(jsc.parallelize(lines,partitionNum));

        Thread thread = new Thread(() -> {
            Random random = new Random();
            while (true){
                queue.add(jsc.parallelize(ImmutableList.of(lines.get(random.nextInt(lines.size()))),partitionNum));
                queue.add(jsc.parallelize(ImmutableList.of(lines.get(random.nextInt(lines.size()))),partitionNum));
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();


        JavaDStream<String> textDS = jssc.queueStream(queue);
        JavaDStream<String> words = textDS.flatMap(new MyFlatMap());
        JavaPairDStream<String, Integer> wordsWithCount = words.mapToPair(new MyPairFunc());
        JavaPairDStream<String, Integer> wordsCount = wordsWithCount.reduceByKey(new MyFunc2());
        wordsCount.foreachRDD(rdd->{
            JavaRDD<String> result = rdd.mapPartitionsWithIndex(new MyMapPartIndexFunc2(), false);
            List<String> collect = result.collect();
            System.out.println(collect.size());
        });

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }





}
