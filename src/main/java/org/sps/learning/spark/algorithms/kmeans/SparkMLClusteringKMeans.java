package org.sps.learning.spark.algorithms.kmeans;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.sps.learning.spark.utils.SparkUtil;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.Arrays;


/**
 *
 * http://spark.apache.org/docs/2.1.0/mllib-clustering.html#k-means
 *
 */
public class SparkMLClusteringKMeans {

    private static final Logger LOGGER = Logger.getLogger(SparkMLClusteringKMeans.class);


    public static void main(String[] args) {

        JavaSparkContext context = SparkUtil.createJavaSparkContext("SparkMLClustering", "local[*]");

        JavaRDD<String> data = context.textFile("./data/gene/data.csv.gz");
        LOGGER.info("count = " + data.count());

        final String  header = data.first();
        data = data.filter(row -> !row.equalsIgnoreCase(header));
        LOGGER.info("count = " + data.count());

        JavaRDD<Vector> parsedData = data.map(line -> {
            String[] sarray = line.split(",");
            double[] values = new double[sarray.length-1];
            for (int i = 1; i < sarray.length; i++) {
                values[i - 1] = Double.parseDouble(sarray[i]);
            }
            return Vectors.dense(values);
        });
        parsedData.cache();

// Cluster the data into two classes using KMeans
        int numClusters = 100;
        int numIterations = 200;
        KMeansModel clusters = org.apache.spark.mllib.clustering.KMeans.train(parsedData.rdd(), numClusters, numIterations);

        System.out.println("Cluster centers:");
        for (Vector center: clusters.clusterCenters()) {
            System.out.println(" " + center);
        }
        double cost = clusters.computeCost(parsedData.rdd());
        System.out.println("Cost: " + cost);

// Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

// Save and load model
        clusters.save(context.sc(), "./hdfs/KMeansModel");
        KMeansModel sameModel = KMeansModel.load(context.sc(),
                "./hdfs/KMeansModel");
    }

}