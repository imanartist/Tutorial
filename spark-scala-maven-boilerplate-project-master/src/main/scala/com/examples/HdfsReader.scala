package com.examples
import org.apache.spark.SparkContext


class HdfsReader extends Reader {  
  var inputpath: String = null
  var filetype: String  = null
  
  def init(hdfsproperties: Map[String,String]):Boolean ={
    inputpath = hdfsproperties("inputpath")
    if (inputpath != null) true else false
  }

  def execute(sc: SparkContext): org.apache.spark.rdd.RDD[String] ={
  val  textFile = sc.textFile(inputpath)
    textFile.take(2).foreach(println)
    return textFile
  }
}  
