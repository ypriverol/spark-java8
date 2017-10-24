package org.sps.learning.spark.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * This is a utility class to create JavaSparkContext 
 * and other objects required by Spark. There are many 
 * ways to create JavaSparkContext object. Here we offer 
 * 2 ways to create it:
 *
 *   1. by using YARN's resource manager host name
 *
 *   2. by using spark master URL, which is expressed as: 
 *
 *           spark://<spark-master-host-name>:7077
 *
 * @author ypriverol
 * Originally posted by Mahmoud Parsian
 *
 */
public class SparkUtil {

    public static JavaSparkContext createJavaSparkContext(String applicationName, String master){
      SparkConf conf = new SparkConf().setAppName(applicationName).setMaster(master);;
      JavaSparkContext ctx = new JavaSparkContext(conf);
      return ctx;
   }
   
   public static String version() {
      return "2.0.0";
   }   
}
