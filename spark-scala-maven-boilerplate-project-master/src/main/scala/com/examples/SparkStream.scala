//
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import org.apache.spark.SparkConf
//import com.datastax.spark.connector._
////import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming._
////import org.apache.spark.streaming.StreamingContext._
//
//class SparkStream extends App {
//
//   def main(arg: Array[String]) {
//   println("start")
//   }
//}
////    if (arg.length < 2) {
////
////	  System.err.println("Usage: SparkStream localhost port")
////    System.exit(1)
////    }
///*
//    val jobName = "WordCount"
//
//	  val conf = new SparkConf().setAppName(jobName).set("spark.cassandra.connection.host", "10.203.238.204").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    //val lines = ssc.socketTextStream(arg(0),arg(1).toInt)
//    val lines = sc.textFile("test")
//    val words = lines.flatMap(line => line.split(" "))
//    val pairs = words.map(word => (word, 1))
//    val counts = pairs.reduceByKey(_ + _)
//    val cp = counts.collect()
//    println(cp)
//    //ssc.start()
//    //ssc.awaitTermination()
////	  val rdd = ssc.cassandraTable("sparkdata", "words")
////	rdd.toArray.foreach(println)
//	//counts.saveToCassandra("sparkdata", "words", SomeColumns("word", "count"));
//	//val rddnew = sc.cassandraTable("sparkdata", "words")
//	//rddnew.toArray.foreach(println)
//  }
//}
//*/