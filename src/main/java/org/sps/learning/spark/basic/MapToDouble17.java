package org.sps.learning.spark.basic;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author Yasset Perez-Riverol (ypriverol@gmail.com)
 * @date 22/10/15
 */
public class MapToDouble17 {

    public static void main(String[] args) throws Exception {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }

        SparkConf conf = new SparkConf()
                .setMaster(master)
                .setAppName("map-double")
                .setSparkHome("SPARK_HOME")
                .set("spark.driver.maxResultSize", "2g");

        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * Generate the integer numbers using java 7 or previous version
         */
        List<Integer> numbers = new ArrayList<Integer>();
        Random randomGenerator = new Random();
        for(int a=0; a < 100000000; a++)
            numbers.add(randomGenerator.nextInt(100));


        JavaRDD<Integer> rdd = sc.parallelize(numbers, 10);

		JavaDoubleRDD result = rdd.mapToDouble(new DoubleFunction<Integer>() {
			public double call(Integer x) {
				double y = (double) x;
				return y * y;
			}
		});

        System.out.println(result.stats().toString());

    }

}
