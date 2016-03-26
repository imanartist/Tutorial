package com.examples

import org.joda.time.Seconds._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object WordCount {
  
  def main(args: Array[String]){
 
  //  val sparkConf = new SparkConf().setAppName("Streaming Word Count").setMaster("local[2]")
    val ssc = new StreamingContext(args(0), "Stream", Seconds(10))
    val lines = ssc.socketTextStream(args(1),args(2).toInt)
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word, 1))
    val counts = pairs.reduceByKey(_ + _)
    counts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}