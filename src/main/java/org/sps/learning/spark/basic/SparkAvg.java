package org.sps.learning.spark.basic;

/**
 * @author Yasset Perez-Riverol (ypriverol@gmail.com)
 * @date 21/10/15
 */

import java.io.Serializable;
import java.util.Arrays;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public final class SparkAvg {

    public static class AvgCount implements Serializable {
        public AvgCount(int total, int num) {
            total_ = total;
            num_ = num;
        }

        public int total_;
        public int num_;

        public float avg() {
            return total_ / (float) num_;
        }
    }

    public static void main(String[] args) throws Exception {

        String master;

        if (args.length > 0)
            master = args[0];
        else
            master = "local";

        /**
         * The first thing a Spark program must do is to create a JavaSparkContext object,
         * which tells Spark how to access a cluster.
         *
         *  1- The AppName will be shown in the cluster UI: Mesos, Spark, ot YARN
         *  2- The master is the name of the machine, we use local if the user run the program in a local machine
         *  3- A property of the number of cores to be use by the software
         */

        SparkConf conf = new SparkConf().setAppName("avg").setMaster(master).set("spark.cores.max", "10");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * Parallelized collections are created by calling JavaSparkContext’s parallelize method on an existing Collection in your driver program.
         * The elements of the collection are copied to form a distributed dataset that can be operated on in parallel.
         *
         * One important parameter for parallel collections is the number of partitions to cut the dataset into. Note: Typically you want 2-4 partitions for each CPU in your cluster.
         * Normally, Spark tries to set the number of partitions (numSlices) automatically based on your cluster. However, you can also set it manually by passing it as a second parameter
         * to parallelize (e.g. sc.parallelize(data, 10)).
         */

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4),10);


        /**
         *
         * Implementing a function that extend the Function2 from org.apache.spark.api.java.function that take the original RDD and apply a function on it
         * in this case add an element to the original count.
         *
         * Spark’s API relies heavily on passing functions in the driver program to run on the cluster. In Java, functions are represented by classes implementing
         * the interfaces in the org.apache.spark.api.java.function package. There are two ways to create such functions:
         *
         *  1-  Implement the Function interfaces in your own class, either as an anonymous inner class or a named one, and pass an instance of it to Spark.
         *      (http://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/api/java/function/package-summary.html)
         *  2- In Java 8, use lambda expressions to concisely define an implementation. (http://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html)
         *
         */

        AvgCount initial = new AvgCount(0,0);

        AvgCount result = rdd.aggregate(initial,
                (AvgCount a, Integer x) -> {
                    a.total_ += x;
                    a.num_++;
                    return a;
                }, (AvgCount a, AvgCount b) -> {
                    a.total_ += b.total_;
                    a.num_   += b.num_;
                    return a;
                }
        );

        System.out.println(result.avg());
        sc.stop();
    }
}
