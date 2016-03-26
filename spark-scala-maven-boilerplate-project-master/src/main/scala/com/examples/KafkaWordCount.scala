//
//
///**
// * @author gur36685
// */
//
//
//
//  
//package com.aricent
//
//import java.util.HashMap
//
//
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka._
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import org.apache.spark.SparkConf
//import com.datastax.spark.connector._
//import com.datastax.spark.connector.streaming._
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.StreamingContext._
//
//
///**
// * Consumes messages from one or more topics in Kafka and does wordcount.
// * Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
// *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
// *   <group> is the name of kafka consumer group
// *   <topics> is a list of one or more kafka topics to consume from
// *   <numThreads> is the number of threads the kafka consumer should use
// *
// * Example:
// *    `$ bin/run-example \
// *      org.apache.spark.examples.streaming.KafkaWordCount zoo01,zoo02,zoo03 \
// *      my-consumer-group topic1,topic2 1`
// */
//object KafkaWordCount {
//  def main(args: Array[String]) {
//    if (args.length < 4) {
//      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
//      System.exit(1)
//    }
//   
//    val Array(zkQuorum, group, topics, numThreads) = args
//    val sparkConf = new SparkConf().setAppName("KafkaWordCount").set("spark.cassandra.connection.host", "10.203.238.204")
//    val ssc = new StreamingContext(sparkConf, Seconds(10))
//    
//    println("****Streaming Context Set*****")
//    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
//    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
//   
//     val words = lines.flatMap(line => line.split(" "))
//    val pairs = words.map(word => (word, 1))
//    val wordCounts = pairs.reduceByKey(_ + _)
//    println("***Calculated Word Count****")
//    
//
//    wordCounts.print()
//    wordCounts.saveToCassandra("sparkdata", "words", SomeColumns("word", "count"));
//  
//    ssc.checkpoint("checkpoint")
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//}
// 
//  
//  