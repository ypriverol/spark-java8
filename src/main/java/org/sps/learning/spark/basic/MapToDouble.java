package org.sps.learning.spark.basic;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * This example provides a way of mapping a JavaRDD to a JavaDoubleRDD.
 * The JavaDoubleRDD can give tou basic statistics about the RDD object like:
 *   1- example application calculates mean, max, min, …etc of a sample dataset.
 *
 * @author Yasset Perez-Riverol (ypriverol@gmail.com)
 * @date 22/10/15
 *
 *
 */
public class MapToDouble {


    public static void main(String[] args) throws Exception {

        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }

        JavaSparkContext sc = new JavaSparkContext(master, "map-double",
                System.getenv("SPARK_HOME"), System.getenv("JARS"));

        List<Integer> random = Collections.unmodifiableList(
                new Random()
                        .ints(-100, 101).limit(100000000)
                        .boxed()
                        .collect(Collectors.toList())
        );


        /**
         * Add more than 100'000'000 of random integers using the Java 8 library for Stream and split them in 10 slides.
         */
        JavaRDD<Integer> rdd = sc.parallelize(random, 10);

        /**
         * The next step will compute for all the members the power 2 and then generate the JavaDoubleRDD with the statistics.
         */
		JavaDoubleRDD result = rdd.mapToDouble( x -> (double)x*x);

        /**
         * Print the statistics for the given DoubleRDD: count, mean stdev, max and min
         */

        System.out.println(result.stats().toString());

    }
}
