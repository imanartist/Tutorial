
package com.examples
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

/**
 * @author gur36685
 */
class Processing {
  
  def executeLogic(rdd: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[(String,Int)] = 
  {
      val counts = rdd.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
      counts     
  }
}