
package com.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

trait Writer {
  
  def init(targetproperties: Map[String,String]): Boolean
  def execute(rdd: org.apache.spark.rdd.RDD[(String,Int)])

}