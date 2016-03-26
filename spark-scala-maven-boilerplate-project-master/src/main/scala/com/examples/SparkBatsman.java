package com.examples;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.hsqldb.jdbc.jdbcBlob;

import scala.Tuple2;


public class SparkBatsman implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2444656849446959618L;

	public static void main2(String args[]){
		SparkBatsman sj = new SparkBatsman();
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
	JavaRDD<Integer> lines  = jsc.parallelize(Arrays.asList(1,2,3,1,3,5));
    JavaDoubleRDD jd = lines.mapToDouble(new DoubleFunction<Integer> (){
     
		@Override
		public double call(Integer arg0) throws Exception {
			// TODO Auto-generated method stub
			return Double.parseDouble(arg0.toString());
		}});
    
    jd.foreach( new VoidFunction<Double>(){

		@Override
		public void call(Double arg0) throws Exception {
			// TODO Auto-generated method stub
			System.out.println("foreach called for"+ arg0);
			arg0 = arg0 * 2.0;
		}
    	
    });
    double d = jd.mean(); 
    double s = jd.sum();
    System.out.println("The mean is:" +d );
    System.out.println("The sum is:" +s);
	JavaRDD<String> strRDD = jsc.textFile("score");
	
	
	JavaPairRDD<String,Integer> wordpair = strRDD.mapToPair(new PairFunction<String,String,Integer>(){

		@Override
		public Tuple2<String, Integer> call(String arg0) throws Exception {
			// TODO Auto-generated method stub
			String[] a = arg0.split(",");
			return new Tuple2<String,Integer>(a[0],Integer.parseInt(a[1]));
		}
		
	});
	
	
	JavaPairRDD<String,Tuple2<Integer,Integer>> jpr = wordpair.mapValues(new Function<Integer,Tuple2<Integer,Integer>>(){

		@Override
		public Tuple2<Integer, Integer> call(Integer arg0) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<Integer,Integer>(arg0,1);
		}
		
	});
	
	JavaPairRDD<String,Tuple2<Integer,Integer>> avgpair  = jpr.reduceByKey(new Function2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>(){

		@Override
		public Tuple2<Integer,Integer> call(Tuple2<Integer,Integer> arg0, Tuple2<Integer,Integer> arg1) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<Integer,Integer>(arg0._1+ arg1._1, arg0._2+arg1._2);
		}

		/**
		 * 
		 */
		
		
	});
	
	JavaPairRDD<String,Double> avg = avgpair.mapToPair( new PairFunction<Tuple2<String,Tuple2<Integer,Integer>>, String, Double>(){
		
		@Override
		public Tuple2<String,Double> call(Tuple2<String, Tuple2<Integer, Integer>> arg0)
				throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<String, Double>(arg0._1,(double) ((arg0._2._1 + arg0._2._2)/2));
		}
		
	});
	
	Tuple2<String,Double> topavg = avg.first();
	System.out.println("Top Batsman is:"+ topavg._1);
	
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
	List<Tuple2<String,Double>> wordCount = avg.collect();
	System.out.println("WordCount without article");
	for(Tuple2<String,Double> tup : wordCount){
	
	System.out.println(tup._1 + "->"+tup._2);
	}
	}

}
	


		
		
	
	

