//package com.examples
//
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import org.apache.spark.SparkConf
//import com.datastax.spark.connector._
//
//object MainExample {
//
//  def main(arg: Array[String]) {
//    
//    println("start")
//
//    if (arg.length < 1) {
//
//	  System.err.println("Usage: MainExample <path-to-files>")
//      System.exit(1)
//    }
//
//    val jobName = "MainExample"
//
//	val conf = new SparkConf().setAppName(jobName).set("spark.cassandra.connection.host", "10.203.238.204")
//    val sc = new SparkContext(conf)
//
//    val pathToFiles = arg(0)
//
//    val textFile = sc.textFile(pathToFiles)
//	textFile.take(2).foreach(println)
//    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
//	val rdd = sc.cassandraTable("sparkdata", "words")
//	rdd.toArray.foreach(println)
//	counts.saveToCassandra("sparkdata", "words", SomeColumns("word", "count"));
//	val rddnew = sc.cassandraTable("sparkdata", "words")
//	rddnew.toArray.foreach(println)
//  }
//}
