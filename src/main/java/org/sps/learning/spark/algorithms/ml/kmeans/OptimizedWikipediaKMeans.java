package org.sps.learning.spark.algorithms.ml.kmeans;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.sps.learning.spark.utils.SparkUtil;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * NOTE: ----------------------------------------
 *       Apache Spark provides distributed K-Means algorithm; 
 *       the purpose of this class is to exercise and understand 
 *       how does K-Means work.
 * END NOTE --------------------------------------
 * 
 * @author Khatwani Parth Bharat (h2016170@pilani.bits-pilani.ac.in)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 * @author ypriverol Has modified the example for Java 8 compatibility
 *
 */
public class OptimizedWikipediaKMeans {

    private static final Logger LOGGER = Logger.getLogger(OptimizedWikipediaKMeans.class);
    private static final String FEATURED_FILE_INOUT = "./hdfs/wikidata/featurized/";
    private static final Integer CLUSTER_NUMBER = 100;
    private static final Double ITERATIONS = 0.90;
    private static final String  OUTPUT_FILE = "./data/wikidata/";

    public static void main(String[] args) throws Exception {

        String wikiData = FEATURED_FILE_INOUT;
        int K = CLUSTER_NUMBER;
        double convergeDist = ITERATIONS;
        BufferedWriter outputWriter = null;
        //
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-YY HH:mm");
        long progSTime = System.currentTimeMillis();
        String outputFile = OUTPUT_FILE + sdf.format(new Date()) + ".txt";

        if (args.length != 4) {
            System.err.println("Usage: OptimizedWikipediaKMeans <file> <k> <iters> <outputfiletime_details> <errorLogFile> <outputfile>");
            System.out.println("Using the default paramters... ");
        }else{
            wikiData = args[0];
            K = Integer.parseInt(args[1]);
            convergeDist = Double.parseDouble(args[2]);
            outputFile = args[3];

        }
        outputWriter = new BufferedWriter(new FileWriter(outputFile + sdf.format(new Date()) + ".txt"));

        JavaSparkContext context = SparkUtil.createJavaSparkContext("OptimizedWikipediaKMeans", "local[2]");

        try {


            long dataReadSTime = System.currentTimeMillis();
            //read the input data
            JavaRDD<Vector> data = getFeatureizedData(wikiData, context);
            LOGGER.info("Number of data records " + data.count());

            //get the initail k centriods
            final List<Vector> centroids = getInitialCentroids(data, K);
            LOGGER.info("Done selecting initial centroids: " + centroids.size());
            long dataReadETime = System.currentTimeMillis();
            LOGGER.info("Total Data Read time is " + (((double) (dataReadETime - dataReadSTime)) / 1000.0) + "\n");

            double tempDist = 1.0 + convergeDist;
            long clusteringSTime = System.currentTimeMillis();
            long itrSTime = System.currentTimeMillis();
            int i = 0;
            while (tempDist > convergeDist) {
                System.out.println("Get the closest points");
                //assign each point to their closest centriod 				
                JavaPairRDD<Integer, Tuple2<Vector, Integer>> closest = getClosest(data, centroids);
                // reduce step
                JavaPairRDD<Integer, Tuple2<Vector, Integer>> pointsGroup = closest.reduceByKey(
                        (Function2<Tuple2<Vector, Integer>, Tuple2<Vector, Integer>, Tuple2<Vector, Integer>>) (arg0, arg1) -> new Tuple2<>(Util.add(arg0._1(), arg1._1()), arg0._2() + arg1._2()));
                System.out.println("after reduce by key");
                //get the new centriods				
                Map<Integer, Vector> newCentroids = getNewCentroids(pointsGroup);
                System.out.println("after getting new centriods");
                //calculate the delta					
                tempDist = Util.getDistance(centroids, newCentroids, K);
                System.out.println("after updating the distance");
                //assign new centriods
                for (Map.Entry<Integer, Vector> t : newCentroids.entrySet()) {
                    centroids.set(t.getKey(), t.getValue());
                }
                System.out.println("after updating centriod list");
                long itrETime = System.currentTimeMillis();

                LOGGER.info("Iteration" + (i + 1) + " took :- " + (((double) (itrETime - itrSTime)) / 1000.0) + "\n");
                LOGGER.info("Finished iteration (delta = " + tempDist + ")");
                itrSTime = itrETime;
                i++;
                System.out.println("loop ends");
            }

            long clusteringETime = System.currentTimeMillis();
            LOGGER.info("Clustering time is " + ((double) (clusteringETime - clusteringSTime)) / (1000.0) + "\n");

            long progETime = System.currentTimeMillis();
            LOGGER.info("Total Program time is " + (((double) (progETime - progSTime)) / 1000.0) + "\n");
            System.out.println("Total Program time is " + (((double) (progETime - progSTime)) / 1000.0) + "\n");
            //
            LOGGER.info("Cluster centriods:");
            outputWriter.write("Cluster centriods:\n");
            for (Vector t : centroids) {
                //System.out.println("" + t.apply(0)+"\t"+t.apply(1)+"\t"+t.apply(2));
                outputWriter.write("" + t.apply(0) + " " + t.apply(1) + " " + t.apply(2) + "\n");
            }
        } 
        finally {
            IOUtils.closeQuietly(outputWriter);
            System.out.println("Stoping context");
            if (context != null) {
                context.stop();
            }
        }
        //
        System.exit(0);
    }

    static Vector average(Vector vec, Integer numVectors) {
        double[] avg = new double[vec.size()];
        for (int i = 0; i < avg.length; i++) {
            // avg[i] = vec.apply(i) * (1.0 / numVectors);
            avg[i] = vec.apply(i) / ((double) numVectors);
        }
        return new DenseVector(avg);
    }

    static JavaRDD<Vector> getFeatureizedData(String wikiData, JavaSparkContext context) {
        return context.textFile(wikiData).map((Function<String, Vector>) arg0 -> Util.buildVector(arg0, "\t")).cache();
    }

    static Map<Integer, Vector> getNewCentroids(JavaPairRDD<Integer, Tuple2<Vector, Integer>> pointsGroup) {
        return pointsGroup.mapValues((Function<Tuple2<Vector, Integer>, Vector>) arg0 -> average(arg0._1, arg0._2)).collectAsMap();

    }

    static JavaPairRDD<Integer, Tuple2<Vector, Integer>> getClosest(JavaRDD<Vector> data, final List<Vector> centroids) {
        return data.mapToPair((PairFunction<Vector, Integer, Tuple2<Vector, Integer>>) in -> new Tuple2<>(Util.closestPoint(in, centroids), new Tuple2<>(in, 1)));
    }

    static List<Vector> getInitialCentroids(JavaRDD<Vector> data, final int K) {
        List<Vector> centroidTuples = data.take(K);
        final List<Vector> centroids = new ArrayList<Vector>();
        for (Vector t : centroidTuples) {
            centroids.add(t);
        }
        return centroids;
    }
}
