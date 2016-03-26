package com.examples
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._


/**
 * @author gur36685
 */
class ConfigSpark {
  
   def startup(configproperties: Map[String,String]): SparkContext={
   val jobname =  configproperties("jobname")
   val ip = configproperties("spark.cassandra.connection.host")
   val conf = new SparkConf().setAppName(jobname).set("spark.cassandra.connection.host", configproperties("spark.cassandra.connection.host"))
   val sc = new SparkContext(conf)
   return sc
  }
  
}