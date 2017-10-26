package org.sps.learning.spark.algorithms.ml.kmeans;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 25/10/2017.
 */
public class FeaturizedOutput implements Serializable{

    private static final Logger LOGGER = Logger.getLogger(FeaturizedOutput.class);


    /**
     *
     * Build the featureized output, to be used by future applications such as K-Means
     *
     * @param featurizedRDD an RDD, where key is a String of the form [projectCode + " " + pageTitle]
     * and value is a list of 24 features (one per hour)
     *
     * @return JavaRDD<String>, which may be used later on for analytics such as K-Means.
     * Each item of the output RDD will have:
     *     <key><#><feature_1><,><feature_2><,>...<,><feature_24>
     *     where <key> is a String of the form [projectCode + " " + pageTitle]
     *
     */

    public JavaRDD<String> buildFeaturizedOutput(final JavaPairRDD<String, double[]> featurizedRDD) {

        return featurizedRDD.map(
                (Function<Tuple2<String, double[]>, String>) kv -> {
                    StringBuilder builder = new StringBuilder();
                    //
                    builder.append(kv._1); // key
                    builder.append(",");   // separator of key from the values/features
                    //
                    double[] data = kv._2;
                    for (int i=0; i < 1; i++) {
                        builder.append(data[i]); // feature
                        builder.append(",");
                    }
                    builder.append(data[1]); // the last feature (24'th)

                    System.out.println(builder.toString());
                    LOGGER.debug("Gaurhari" + builder.toString());

                    //
                    return builder.toString();
                });
    }


}

