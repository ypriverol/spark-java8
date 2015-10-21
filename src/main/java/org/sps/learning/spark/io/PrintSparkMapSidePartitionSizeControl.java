package org.sps.learning.spark.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Assert;


public class PrintSparkMapSidePartitionSizeControl {
    public static int ONE_MB = 1024*1024;
    public static void printSizeOfEachPartition(String inputPath, long partitionSize,boolean splitOverall) throws IOException{
        Assert.assertTrue(partitionSize>0);
        FileSystem fs = FileSystem.get(new Configuration());
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(inputPath), true);
        
        List<Long> partitions = new ArrayList<Long>();
        while(files.hasNext()){
            FileStatus f = files.next();
            if (!f.isDirectory()) {
               
               long fSize = f.getLen();
               System.out.println("Input File Name:"+f.getPath().toString()+"="+fSize);
               while(fSize>=partitionSize){
                   partitions.add(partitionSize);
                   fSize=fSize-partitionSize;
               }
               if(fSize>0){
                   partitions.add(fSize);
                   fSize=fSize-partitionSize;
               }
            }               
       }
       int partitionNo = 0;
       long pSize = 0;
       for(long p:partitions){         
           if(!splitOverall){              
               pSize = p;
               System.out.println("Partition Index="+partitionNo+", Partition Size="+pSize/ONE_MB);
               partitionNo++;
           }
           else{
               if(p<partitionSize){
                   pSize = pSize + p;
               }
               else{
                   pSize = pSize + p;                  
                   System.out.println("Partition Index="+partitionNo+", Partition Size="+pSize/ONE_MB);
                   pSize = 0;
                   partitionNo++;
               }
           }
       }
       if(splitOverall && pSize>0)
           System.out.println("Partition Index="+partitionNo+", Partition Size="+pSize/ONE_MB);

    }
	public static void print(String description,String inputPath,
			                 int noOfPartitions,long blockSize, long minPartitionSize) throws Exception{
		long oneMB = SparkMapSidePartitionSizeControl.ONE_MB;
		if(blockSize==0){
			blockSize = 32 * oneMB;
		}

		System.out.println("****"+description+"*************");		
		long goalSize = SparkMapSidePartitionSizeControl.getGoalSize(inputPath,noOfPartitions);
		long partitionSize = SparkMapSidePartitionSizeControl.computePartitionSize(goalSize,minPartitionSize,blockSize);

		
		System.out.println("Goal Size in MB="+goalSize/oneMB);
		System.out.println("Partition Size in MB="+partitionSize/oneMB);
		System.out.println("Now printing each file, partition index and the size of the partition");
		SparkMapSidePartitionSizeControl.printSizeOfEachPartition(inputPath,partitionSize,goalSize<blockSize);
		System.out.println("***************************");
	}
	public static void main(String[] args) throws Exception {
		String inputPath = args[0];
		/*
		 * Scenario 1=Default
		 */
		String description = "Scenario 1=Default";
		int noOfPartitions = 0;//Default
		long blockSize = 0;//32MB by default
		long minPartitionSize = 0;//Default
		PrintSparkMapSidePartitionSizeControl.print(description, inputPath, noOfPartitions, blockSize, minPartitionSize);

		/*
         * Scenario 1.1=Default
         */
        description = "Scenario 1.1=Default";
        noOfPartitions = 0;//Default
        blockSize = 64 * 1024 * 1024;//64MB
        minPartitionSize = 0;//Default
        PrintSparkMapSidePartitionSizeControl.print(description, inputPath, noOfPartitions, blockSize, minPartitionSize);

        /*
         * Scenario 2=Control Number of Partitions. 
         * NoOfPartions = 30
         * Block Size In Local Mode = 32 MB
         * minPartitionSize=1 byte(Defined in Hadoop code)
         */
        description = "Scenario 2";
        noOfPartitions = 30;
        blockSize = 0;//default
        minPartitionSize = 0;//default
        PrintSparkMapSidePartitionSizeControl.print(description, inputPath, noOfPartitions, blockSize, minPartitionSize);

        /*
		 * Scenario 3=Increase Partition Size(The Wrong Way)
		 * NoOfPartions = 5
		 * Block Size In Local Mode = 32MB
		 * minPartitionSize=1 byte(Defined in Hadoop code)
		 */
		description = "Scenario 3=Increase Partition Size(The Wrong Way)";
		noOfPartitions = 5;
		blockSize = 0;//Default
		minPartitionSize = 0;//Default
		PrintSparkMapSidePartitionSizeControl.print(description, inputPath, noOfPartitions, blockSize, minPartitionSize);

		/*
		 * Scenario 4=Increase Partition Size(The Right Way)
		 * NoOfPartions = 0
		 * Block Size In Local Mode = 32MB
		 * minPartitionSize= 64MB
		 */
		description = "Scenario 4=Increase Partition Size(The Right Way)";
		noOfPartitions = 0;//Default
		blockSize = 0;//Default
		minPartitionSize = 64 * 1024 * 1024;
		PrintSparkMapSidePartitionSizeControl.print(description, inputPath, noOfPartitions, blockSize, minPartitionSize);
		
	}
}
