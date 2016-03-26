//package com.examples
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import org.apache.spark.SparkConf
//import com.datastax.spark.connector._
//
//
///**
// * @author gur36685
// */
//object TestController {
//  
//  def main(arg: Array[String])
//  {
//    
//    /* Initialize Maps */
//    val configparameter = Map ("jobname" -> "wordcount", "spark.cassandra.connection.host" -> "10.203.238.204"  )
//    val inputparameterSource = Map("inputpath" -> "/demo")
//    val inputparameterTarget = Map("keyspace" -> "sparkdata", "table" -> "words", "column1" -> "word", "column2" -> "count")
// 
//    /* start sparkcontext */
//    val sparkconfigobj = new ConfigSpark()
//    val sc = sparkconfigobj.startup(configparameter)
//    
//    /*initialize source */
//    val hdfsobj = new HdfsReader()
//    val checkhdfsinit= hdfsobj.init(inputparameterSource)
//    
//    /*execute source */
//    val hdfsrdd = hdfsobj.execute(sc)
//    
//    /*execute processing */
//    val processingobj = new Processing()
//    val processedrdd = processingobj.executeLogic(hdfsrdd)
//    
//    /* initialize target */
//    val cassandraobj = new CassandraWriter()
//    val checkcassandrainit =  cassandraobj.init(inputparameterTarget)
//    
//    /*execute target */
//    cassandraobj.execute(processedrdd)
//    
//    
//  }
//  
//  
//}