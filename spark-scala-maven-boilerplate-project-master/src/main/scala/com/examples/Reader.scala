package com.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._


trait Reader {
  
  def init(sourceproperties: Map[String,String]): Boolean 
  def execute(sc: SparkContext): org.apache.spark.rdd.RDD[String]
 
}
  