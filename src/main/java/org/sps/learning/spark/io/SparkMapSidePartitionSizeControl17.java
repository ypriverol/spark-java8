package org.sps.learning.spark.io;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkMapSidePartitionSizeControl17 {

	public static int ONE_MB = 1024*1024;
	
			                           
	public static void controlMapSidePartitionSize(String inputPath,
			                                       String outputPath,
			                                       int noOfPartitions,
			                                       long batchSize,			                                       
			                                       long minPartitionSize) throws Exception
	{		
		JavaSparkContext sc = new JavaSparkContext("local", "partitionsizecontrol");
		if(batchSize>0){
			sc.hadoopConfiguration().setLong("fs.local.block.size", batchSize);	
		}
		if(minPartitionSize>0){
			sc.hadoopConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize", 
											 minPartitionSize);	
		}		
		FileUtils.deleteQuietly(new File(outputPath));
		JavaRDD<String> rdd = null;
		if(noOfPartitions>0){
			rdd = sc.textFile(inputPath,noOfPartitions);
		}
		else{
			rdd = sc.textFile(inputPath);
		}
		System.out.println(rdd.partitions().size());
		JavaRDD<String> mapRdd = rdd.map(new Function<String,String>() {
            public String call(String l) {
                return l;
            }
        });
		mapRdd.saveAsTextFile(outputPath);
		sc.close();
	}

	public static long getGoalSize(String inputPath,int noOfPartitions) throws IOException{
		FileSystem fs = FileSystem.get(new Configuration());
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(inputPath), true);
		long totalSize = 0;
		while(files.hasNext()){
			FileStatus f = files.next();
	    	if (!f.isDirectory()) {
	           totalSize += f.getLen();
	        }	            
	   }
	   System.out.println("Total Size in MB="+totalSize/ONE_MB);
	   long goalSize = Math.round((float)totalSize / (noOfPartitions == 0 ? 1 : noOfPartitions));
	   return goalSize;
	}
	
	public static long computePartitionSize(long goalSize, long minPartitionSize,long blockSize) {
	   
	   if(blockSize==0){
		   blockSize = 32 * 1024 * 1024;
	   }
	   if(minPartitionSize==0){
		   minPartitionSize = 1;
	   }
	   long partitionSize = Math.max(minPartitionSize, Math.min(goalSize, blockSize));
	   System.out.println(partitionSize+"=Math.max(minSize="+minPartitionSize+", Math.min(goalSize="+goalSize+", blockSize="+blockSize+"))");
	   return partitionSize;
	}
	

	
	
	
	public static void main(String[] args) throws Exception {
		///System.setProperty("fs.local.block.size", Integer.toString(64*1024*1024));		
		System.out.println(System.getProperty("hadoop.home.dir"));

		String inputPath = args[0];
		String baseOutputPath = args[1];
		
		int scenarioIndex=1;
		/*
		 * Scenario 1=Default
		 * NoOfPartions = Default
		 * Block Size In Local Mode = 32MB
		 * minPartitionSize=1 byte(Defined in Hadoop code)
		 */
		int noOfPartitions = 0;
		long blockSize = 0;//32MB by default
		long minPartitionSize = 0;
		long goalSize = getGoalSize(inputPath,noOfPartitions);
		long partitionSize = computePartitionSize(goalSize,minPartitionSize,blockSize);
		System.out.println("Partition Size in MB="+partitionSize/ONE_MB);
		//SparkMapSidePartitionSizeControl.controlMapSidePartitionSize(inputPath, outputPathPrefix+scenarioIndex,noOfPartitions,blockSize,minPartitionSize);
		scenarioIndex++;
		
        /*
         * Scenario 2=Control Number of Partitions. 
         * NoOfPartions = 30
         * Block Size In Local Mode = 32 MB
         * minPartitionSize=1 byte(Defined in Hadoop code)
         */
        noOfPartitions = 30;
        blockSize = 0;//default
        minPartitionSize = 0;//default	
        goalSize = getGoalSize(inputPath,noOfPartitions);
        partitionSize = computePartitionSize(goalSize,minPartitionSize,blockSize);
        System.out.println("Partition Size in MB="+partitionSize/ONE_MB);

        SparkMapSidePartitionSizeControl17.controlMapSidePartitionSize(inputPath, baseOutputPath + scenarioIndex, noOfPartitions, blockSize, minPartitionSize);
		scenarioIndex++;

        /*
         * Scenario 3=Increase Partition Size(The Wrong Way)
         * NoOfPartions = 5
         * Block Size In Local Mode = 32MB
         * minPartitionSize=1 byte(Defined in Hadoop code)
         */
        noOfPartitions = 5;
        blockSize = 0;//Default
        minPartitionSize = 0;//Default
        goalSize = getGoalSize(inputPath,noOfPartitions);
        partitionSize = computePartitionSize(goalSize,minPartitionSize,blockSize);
        System.out.println("Partition Size in MB="+partitionSize/ONE_MB);

        SparkMapSidePartitionSizeControl17.controlMapSidePartitionSize(inputPath, baseOutputPath + scenarioIndex, noOfPartitions, blockSize, minPartitionSize);
		scenarioIndex++;

        /*
         * Scenario 4=Increase Partition Size(The Right Way)
         * NoOfPartions = 0
         * Block Size In Local Mode = 32MB
         * minPartitionSize= 64MB
         */
        noOfPartitions = 0;//Default
        blockSize = 0;//Default
        minPartitionSize = 64 * 1024 * 1024;
        goalSize = getGoalSize(inputPath,noOfPartitions);
        partitionSize = computePartitionSize(goalSize,minPartitionSize,blockSize);
        System.out.println("Partition Size in MB="+partitionSize/ONE_MB);

        SparkMapSidePartitionSizeControl17.controlMapSidePartitionSize(inputPath, baseOutputPath + scenarioIndex, noOfPartitions, blockSize, minPartitionSize);
		scenarioIndex++;
		
	}
}
