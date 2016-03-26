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
//import scala.util.parsing.json._
//import java.util.Calendar
//import java.util.Date
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
//case class Coordinate(id: String, ax: String, ay: String, az: String, oa: String, ob: String, og:String, time_value: Date)
//
//object IotData {
//  def main(args: Array[String]) {
//    if (args.length < 4) {
//      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
//      System.exit(1)
//    }
//   
//    val Array(zkQuorum, group, topics, numThreads) = args
//    val sparkConf = new SparkConf().setAppName("IoTData").set("spark.cassandra.connection.host", "10.203.238.204")
//    val ssc = new StreamingContext(sparkConf, Seconds(1))
//    
//    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
//    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
//    val jsonf = lines.map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[String, Any]]) 
//
// 
// 
//
//   def mapToRecord(jsonMap : Map[String,Any]): Coordinate = {
//  
//    val id = jsonMap.get("id").get.asInstanceOf[String]
//    val coordinate = jsonMap.get("d").get.asInstanceOf[scala.collection.immutable.Map[String, Any]]
//    val ay = coordinate.get("ay").get.asInstanceOf[String]
//    val az = coordinate.get("az").get.asInstanceOf[String]
//    val ax = coordinate.get("ax").get.asInstanceOf[String]
//    val oa = coordinate.get("oa").get.asInstanceOf[String]
//    val ob = coordinate.get("ob").get.asInstanceOf[String]
//    val og = coordinate.get("og").get.asInstanceOf[String]
//    val timenow = Calendar.getInstance().getTime()
//    val record = Coordinate(id,ax,ay,az,oa,ob,og,timenow)
//    record
//   
//   }
//  
//   val coordinate = jsonf.map(m => mapToRecord(m))
//
//   coordinate.saveToCassandra("iotdata", "coordinate", SomeColumns("id", "ax","ay", "az", "oa", "ob", "og","time_value"));
//  
//   ssc.checkpoint("checkpoint")
//   ssc.start()
//   ssc.awaitTermination()
//  }
//
//}
// 
//  
//  