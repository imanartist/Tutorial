package com.examples;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class SparkFriends implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2444656849446959618L;

	public static void main1(String args[]){
		SparkFriends sj = new SparkFriends();
		sj.startJob();
		
	}
	
	public void startJob(){
		
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("TestApp");
		
		SparkContext sc = new SparkContext(conf);
		
		JavaSparkContext jsc = new JavaSparkContext(sc);
			
       //JavaRDD<String> strRDD  = jsc.parallelize(Arrays.asList("A,B","B,C", "C,D", "D,E", "A,F", "C,F"));
	
    JavaRDD<String> strRDD = jsc.textFile("friends");
    Long c = strRDD.count();
    System.out.println("The number of strings" + c);
	List<String> li = strRDD.collect();
	for(String a : li){
		System.out.println(a);
	}
//	JavaRDD<Tuple2<String,String>> prdd = strRDD.map(new Function<String,Tuple2<String,String>>(){
//
//		private static final long serialVersionUID = 359797511547629172L;
//
//		@Override
//		public Tuple2<String, String> call(String arg0) throws Exception {
//			String[] two = arg0.split(",");
//		    
//			return new Tuple2<String,String>(two[0],two[1]);
//		}		
//		
//	});
//		
	JavaPairRDD<String,String> wordpair1 = strRDD.mapToPair(new PairFunction<String,String,String>(){

		@Override
		public Tuple2<String, String> call(String arg0) throws Exception {
			String[] two = arg0.split(" ");
			System.out.println(two.length);
			if(two.length == 2){
   			return new Tuple2<String,String>(two[0],two[1]);
			}
			else {
				return new Tuple2<String,String>("ex","ex");
			}
		}		
		
	});
	
	JavaPairRDD<String,String>  wordpair2 = strRDD.mapToPair(new PairFunction<String,String,String>(){

		@Override
		public Tuple2<String, String> call(String arg0) throws Exception {
			String[] two = arg0.split(",");
			if(two.length == 2){
	   			return new Tuple2<String,String>(two[0],two[1]);
				}
				else {
					return new Tuple2<String,String>("ex","ex");
				}
		}		
		
	});
	
	JavaPairRDD<String,String> pair1 = wordpair1.reduceByKey(new Function2<String, String,String>(){

		@Override
		public String call(String arg0, String arg1) throws Exception {
			// TODO Auto-generated method stub
			return arg0.concat(arg1);
		}
		
	});
	
	JavaPairRDD<String,String> pair2 = wordpair1.reduceByKey(new Function2<String, String,String>(){

		@Override
		public String call(String arg0, String arg1) throws Exception {
			// TODO Auto-generated method stub
			return arg0.concat(arg1);
		}
		
	});
	
	JavaPairRDD<String,Integer> count1 = pair1.mapToPair(new PairFunction<Tuple2<String,String>,String,Integer>(){

		@Override
		public Tuple2<String, Integer> call(Tuple2<String,String> arg0) throws Exception {
			int len = arg0._2.length();
   			return new Tuple2<String,Integer>(arg0._1,len);
		}
		});
	
	JavaPairRDD<String,Integer> count2 = pair2.mapToPair(new PairFunction<Tuple2<String,String>,String,Integer>(){

		@Override
		public Tuple2<String, Integer> call(Tuple2<String,String> arg0) throws Exception {
			int len = arg0._2.length();
   			return new Tuple2<String,Integer>(arg0._1,len);
		}
		});
		
	
	JavaPairRDD<String,Tuple2<Integer,Integer>> friendcount = count1.join(count2);
	JavaPairRDD<String,Integer> friends = friendcount.mapToPair(new PairFunction<Tuple2<String,Tuple2<Integer,Integer>>,String, Integer>(){

		@Override
		public Tuple2<String, Integer> call(
				Tuple2<String, Tuple2<Integer, Integer>> arg0) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<String,Integer>(arg0._1,arg0._2._1+ arg0._2._1);
		}
		
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
	List<Tuple2<String,Integer>> friendCount = friends.collect();

	
	System.out.println("Friends");
	for(Tuple2<String,Integer> tup : friendCount){
	
	System.out.println(tup._1 + "->"+tup._2);
	}
	}

}
	


		
		
	
	


