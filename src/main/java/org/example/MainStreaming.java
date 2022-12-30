package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class MainStreaming {
    public static void main(String[] args) throws InterruptedException {
        // spark, java, Windows configuration
        System.setProperty("hadoop.home.dir", "c:/Program Files/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.spark-project").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(15));


        JavaReceiverInputDStream<String> socketReceiver = sc.socketTextStream("localhost", 8989);
        JavaDStream<String> dStream = socketReceiver;
        dStream
            .window(Durations.seconds(60))
            .print();

        sc.start();
        sc.awaitTermination();
    }

}
