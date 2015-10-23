package org.sps.learning.spark.basic;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;


/**
 * This is a simple example of Spark of a counter, well explained and verbose about Spark and it components.
 * The word count example read a file from the an input file and count all the words in the file. The present version is based in java 8 lambda expressions
 *
 * Readme some extra documentation:
 *   1- http://stackoverflow.com/questions/19620642/failed-to-locate-the-winutils-binary-in-the-hadoop-binary-path
 */

public class SparkWordCount {

    public static void main(String[] args) throws Exception {

		System.out.println(System.getProperty("hadoop.home.dir"));

        String inputPath = args[0];
		String outputPath = args[1];

		FileUtils.deleteQuietly(new File(outputPath));

        /**
         * The first thing a Spark program must do is to create a JavaSparkContext object,
         * which tells Spark how to access a cluster.
         *
         *  1- The AppName will be shown in the cluster UI: Mesos, Spark, ot YARN
         *  2- The master is the name of the machine, we use local if the user run the program in a local machine
         *  3- A property of the number of cores to be use by the software
         */


        SparkConf conf = new SparkConf().setAppName("word-counter").setMaster("local").set("spark.cores.max", "10");
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
         * This function alows you to filter the JavaPairRDD for all the elements that the number
         * of occurrences are bigger than 20.
         */

        Function<Tuple2<String, Integer>, Boolean> filterShortest = new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return (stringIntegerTuple2._2() > 20);
            }
        };

        /**
         * This function allow to compute the number of occurrences for a particular word, the first instruction flatMap allows to create the key of the map by splitting
         * each line of the JavaRDD. Map to pair do not do anything because it only define that a map will be done after the reduce function reduceByKey.
         */

		JavaPairRDD<String, Integer> counts = rdd
                .flatMap(x -> Arrays.asList(x.split(" ")))
                .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
                .reduceByKey((x, y) -> x + y)
                .filter(filterShortest);


        /**
         * The RDDs can be save to a text file by using the saveAsTextFile function which export the RDD information to a text representation,
         * either a local path on the machine, or a hdfs://, s3n://, etc URI)
         */
		counts.saveAsTextFile(outputPath);
		sc.close();

	}


}
