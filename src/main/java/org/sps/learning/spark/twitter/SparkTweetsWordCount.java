package org.sps.learning.spark.twitter;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.List;


/**
 * This is a simple example of Spark of a counter, well explained and verbose about Spark and it components.
 * The word count example read a file from the an input file and count all the words in the file. The present version is based in java 8 lambda expressions
 *
 * Readme some extra documentation:
 *   1- http://stackoverflow.com/questions/19620642/failed-to-locate-the-winutils-binary-in-the-hadoop-binary-path
 */

public class SparkTweetsWordCount {

    final static String FILE_INPUT = "./data/tweets.txt";
    final static String FILE_OUTPUT = "./data/tweets-word-count.txt";

    public static void main(String[] args) throws Exception {

		System.out.println(System.getProperty("hadoop.home.dir"));

        String inputPath = FILE_INPUT;
        String outputPath = FILE_OUTPUT;
        if(args.length == 2){
             inputPath = args[0];
             outputPath = args[1];
        }

		FileUtils.deleteQuietly(new File(outputPath));

        /**
         * The first thing a Spark program must do is to create a JavaSparkContext object,
         * which tells Spark how to access a cluster.
         *
         *  1- The AppName will be shown in the cluster UI: Mesos, Spark, ot YARN
         *  2- The master is the name of the machine, we use local if the user run the program in a local machine
         *  3- A property of the number of cores to be use by the software
         */


        SparkConf conf = new SparkConf().setAppName("Tweets Word Count").setMaster("local").set("spark.cores.max", "10");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * Text file RDDs can be created using SparkContext’s textFile method textFile function. This method takes an URI for the file
         * either a local path on the machine, or a hdfs://, s3n://, etc URI) and reads it as a collection of lines. Every line in the file is
         * an element in the JavaRDD list.
         *
         * Important **Note**: This line defines a base RDD from an external file. This dataset is not loaded in memory or otherwise acted on:
         * lines is merely a pointer to the file.
         *
         * Here is an example invocation:
         */

		JavaRDD<String> rdd = sc.textFile(inputPath);

        /**
         * The function collect, will get all the elements in the RDD into memory for us to work with them.
         * For this reason it has to be used with care, specially when working with large RDDs. In the present
         * example we will filter all the words that contain @ to check all the references to other users in twitter.
         *
         * The function collect return a List
         */

        JavaPairRDD<String, Integer> counts = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Integer>> finalCounts = counts.filter((x) -> x._1().contains("@"))
                .collect();

        for(Tuple2<String, Integer> count: finalCounts)
                System.out.println(count._1() + " " + count._2());

        /**
         * This function allow to compute the number of occurrences for a particular word, the first instruction flatMap allows to create the key of the map by splitting
         * each line of the JavaRDD. Map to pair do not do anything because it only define that a map will be done after the reduce function reduceByKey.
         *
         */

		 counts = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                 .mapToPair(x -> new Tuple2<>(x, 1))
                 .reduceByKey((x, y) -> x + y);

        /**
         * This function allows you to filter the JavaPairRDD for all the elements that the number
         * of occurrences are bigger than 20.
         */

        counts = counts.filter((x) -> x._2() > 20);

        long time = System.currentTimeMillis();
        long countEntries = counts.count();
        System.out.println(countEntries + ": " + String.valueOf(System.currentTimeMillis() - time));

        /**
         * The RDDs can be save to a text file by using the saveAsTextFile function which export the RDD information to a text representation,
         * either a local path on the machine, or a hdfs://, s3n://, etc URI)
         */
		counts.map( x-> x._1() + "," + x._2()).coalesce(1,true).saveAsTextFile(outputPath);
		sc.close();

	}


}
