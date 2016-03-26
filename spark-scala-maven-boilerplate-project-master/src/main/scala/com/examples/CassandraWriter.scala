package com.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._




/**
 * @author gur36685
 */
class CassandraWriter extends Writer {
  
   var keyspace: String = null
   var table: String = null 
   var column1: String = null 
   var column2: String = null
   
  def init(cassandraproperties: Map[String,String]):Boolean = {
    keyspace = cassandraproperties("keyspace")
    table = cassandraproperties("table")
    column1 = cassandraproperties("column1")
    column2 = cassandraproperties("column2")
    if ( keyspace != null && table != null && column1 != null && column2 != null ) true else false   
  }

  def execute(processedrdd: org.apache.spark.rdd.RDD[(String,Int)]) {
    
  processedrdd.saveToCassandra(keyspace, table, SomeColumns(column1, column2));
  
  }
  
}