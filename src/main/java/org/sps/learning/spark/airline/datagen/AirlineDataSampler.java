package org.sps.learning.spark.airline.datagen;

import java.io.File;
import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

//http://stackoverflow.com/questions/19620642/failed-to-locate-the-winutils-binary-in-the-hadoop-binary-path
public class AirlineDataSampler {

	public static class CustomPartitioner extends Partitioner implements Serializable{
		private static final long serialVersionUID = 1L;
		private int partitions;
		public CustomPartitioner(int noOfPartitioners){
			partitions=noOfPartitioners; 
		}
		@Override
		public int getPartition(Object key) {
			String[] sa = StringUtils.splitPreserveAllTokens(key.toString(), ',');
			int y = (Integer.parseInt(sa[0])-1987);
			return (y%partitions);
		}

		@Override
		public int numPartitions() {
			return partitions;
		}		
	}
	
	public static class CustomComparator implements Comparator,Serializable{
		private static final long serialVersionUID = 1L;
		@Override
		public int compare(Object o1, Object o2) {
			String s1 = (String) o1;
			String s2 = (String) o2;
			String[] p1 = StringUtils.splitPreserveAllTokens(s1, ',');
			String[] p2 = StringUtils.splitPreserveAllTokens(s2, ',');
			Integer y1 = Integer.parseInt(p1[0]);
			Integer y2 = Integer.parseInt(p2[0]);
			int result = y1.compareTo(y2);
			if(result==0){
				Integer m1 = Integer.parseInt(p1[1]);
				Integer m2 = Integer.parseInt(p2[1]);
				result = m1.compareTo(m2);
			}			
			if(result==0){
				Integer d1 = Integer.parseInt(p1[2]);
				Integer d2 = Integer.parseInt(p2[2]);
				result = d1.compareTo(d2);
			}			
			return result;
		}		
	}
	
	@SuppressWarnings("resource")
	public static void main(String[] args) throws Exception {
		System.out.println(System.getProperty("hadoop.home.dir"));
		
		String inputPath = args[0];
		String outputPath = args[1];
		float sampledAmt = Float.parseFloat(args[2]);
		
		/*Identify the original number of partitions*/
		String[] extensions = {"bz2"};		
		Collection<File> files = FileUtils.listFiles(new File(inputPath), extensions, false);
		int noOfPartitions = files.size();
		
		/*Delete output file. Do not do this in Production*/		
		FileUtils.deleteQuietly(new File(outputPath));

		/*Initialize Spark Context*/
		JavaSparkContext sc = new JavaSparkContext("local", "airlinedatasampler");
		
		
		/*Read in the data*/
		JavaRDD<String> rdd = sc.textFile(inputPath);

//		/*Process the data*/
//		rdd.filter(l->!(l.startsWith("Year")))  //Skip the header line
//				       .sample(false, sampledAmt)
//				       //.repartition(20)
//				       .mapToPair(l->{
//				    	   String[] parts = StringUtils.splitPreserveAllTokens(l, ",");
//				           String yrMoDd = parts[0]+","+parts[1]+","+parts[2];
//				           return new Tuple2<String,String>(yrMoDd,l);
//				        })	//Map to key-value pair
//				        .repartitionAndSortWithinPartitions(
//				        		new CustomPartitioner(noOfPartitions),//Partition the output as the original input
//				        		new CustomComparator()) //Sort in ascending order by Year,Month and Day
//				        .map(t->t._2()) //Process just the value
//				        .saveAsTextFile(outputPath); //Write to file
//		/*Close the context*/
		sc.close();
	}
}
