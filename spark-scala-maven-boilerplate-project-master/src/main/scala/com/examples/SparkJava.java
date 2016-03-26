package com.examples;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class SparkJava implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2444656849446959618L;

	public static void test(String args[]){
		SparkJava sj = new SparkJava();
		sj.startJob();
		
	}
	
	public void startJob(){
		
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("TestApp");
		
		SparkContext sc = new SparkContext(conf);
		
		JavaSparkContext jsc = new JavaSparkContext(sc);
//		List<String> lines = new 
//				
//	JavaRDD<Integer> lines  = jsc.parallelize(Arrays.asList(1,2,3,1,3,5));
	
	JavaRDD<String> strRDD = jsc.textFile("test");
	JavaRDD<String> prdd = strRDD.flatMap(new FlatMapFunction<String,String>(){

		private static final long serialVersionUID = 359797511547629172L;

		@Override
		public Iterable<String> call(String arg0) throws Exception {
			String[] a = arg0.split(" ");
			List<String> alist = new LinkedList<String>(Arrays.asList(a));
			if (alist.contains("a")){
				alist.remove("a");
			}
			if (alist.contains("an"))
			{
				alist.remove("an");
			}
			if (alist.contains("the")){
				alist.remove("the");
			}
			return alist;
		}
		
	});
	
	JavaPairRDD<String,Integer> wordpair = prdd.mapToPair(new PairFunction<String,String,Integer>(){

		@Override
		public Tuple2<String, Integer> call(String arg0) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<String,Integer>(arg0,1);
		}
		
	});
	
	JavaPairRDD<String,Integer> wordcount = wordpair.reduceByKey(new Function2<Integer,Integer,Integer>(){

		@Override
		public Integer call(Integer arg0, Integer arg1) throws Exception {
			// TODO Auto-generated method stub
			return arg0+arg1;
		}

		/**
		 * 
		 */
		
		
	});
	
	
	
//	List<String> abc = strRDD.collect();
//	for(String a: abc){
//		System.out.println(a);
//	}
//	PairFunction<Integer,Integer,Integer> pf = new PairFunction<Integer,Integer,Integer> (){
//
//		/**
//		 * 
//		 */
//		private static final long serialVersionUID = 1L;
//
//		@Override
//		public Tuple2<Integer, Integer> call(Integer arg0) throws Exception {
//			// TODO Auto-generated method stub
//			return new Tuple2<Integer,Integer>(arg0,1);
//		}
//	}	;
	
//	JavaPairRDD<Integer,Integer> jpr = lines.mapToPair(new PairFunction<Integer,Integer,Integer> (){
//		
//
//		/**
//		 * 
//		 */
//		private static final long serialVersionUID = 235626741791812706L;
//
//		@Override
//		public Tuple2<Integer, Integer> call(Integer arg0) throws Exception {
//			// TODO Auto-generated method stub
//			if ( arg0 == 1){
//				return new Tuple2<Integer,Integer>(arg0,0);
//			}
//			return new Tuple2<Integer,Integer>(arg0,1);
//		}
//	}	
//			).reduceByKey(new Function2<Integer,Integer,Integer>(){
//
//		@Override
//		public Integer call(Integer arg0, Integer arg1) throws Exception {
//			// TODO Auto-generated method stub
//			return arg0 + arg1;
//		}
//		
//	});
//	
	List<Tuple2<String,Integer>> wordCount = wordcount.collect();
	System.out.println("WordCount without article");
	for(Tuple2<String,Integer> tup : wordCount){
	
	System.out.println(tup._1 + "->"+tup._2);
	}
	}

}
	


		
		
	
	

