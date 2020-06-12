package com.bigdata.streaming.spark.java.core;

import com.bigdata.streaming.spark.scala.core.SparkHelper;
import com.bigdata.streaming.spark.scala.streaming.StreamingDemo;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class JavaStreamingWCDemo {

    public static void main(String[] args) throws InterruptedException {
        StreamingDemo.MainArgs mainArgs = StreamingDemo.parseMain(args);
        JavaStreamingContext jssc = getJavaStreamingContext(mainArgs);
        SparkConf conf = jssc.sparkContext().getConf()
            .set("spark.network.timeout","300s")
            .set("spark.network.timeoutInterval","300s")
            .set("spark.storage.blockManagerSlaveTimeoutMs","600s")
            .set("spark.storage.blockManagerTimeoutIntervalMs","600s")

        ;

        JavaReceiverInputDStream<String> javaDS = jssc.socketTextStream(mainArgs.host(), mainArgs.port());

        SparkHelper.doDStreamProcess(jssc.ssc(),javaDS.receiverInputDStream(),mainArgs.minPartitions());
        jssc.start();
        jssc.awaitTermination();
        System.out.println("jssc.await阻塞等待结束, 运行完毕! ");

    }

    private static JavaStreamingContext getJavaStreamingContext(StreamingDemo.MainArgs mainArgs) {
        SparkConf sparkConf = new SparkConf().setAppName(JavaStreamingWCDemo.class.getSimpleName());
        sparkConf.setMaster("local["+mainArgs.cores()+"]");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000 * mainArgs.batchInterval()));
        jssc.sparkContext().setLogLevel(mainArgs.logLevel());
        return jssc;
    }

}
