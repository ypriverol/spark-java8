package org.sps.learning.spark.sampling;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;


/**
 * @author Yasset Perez-Riverol (ypriverol@gmail.com)
 * @date 27/10/15
 */
public class SparkRDDSampling {


    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("sampling").setMaster("local").set("spark.cores.max", "10");
        JavaSparkContext sc = new JavaSparkContext(conf);

        File outputFile = downloadFile("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data.gz", "kddcup.data.gz");


        JavaRDD<String> rawData = sc.textFile(outputFile.getAbsolutePath());

        /**
         * Sampling RDDs: In Spark, there are two sampling operations, the transformation sample and the action
         * takeSample. By using a transformation we can tell Spark to apply successive transformation on a sample
         * of a given RDD. By using an action we retrieve a given sample and we can have it in local memory to be
         * used by any other standard library. The sample transformation takes up to three parameters.
         * First is weather the sampling is done with replacement or not.
         * Second is the sample size as a fraction. Finally we can optionally provide a random seed.
         */


        JavaRDD<String> sampledData = rawData.sample(false, 0.1, 1234);
        long sampleDataSize = sampledData.count();
        long rawDataSize = rawData.count();
        System.out.println(rawDataSize + " and after the sampling: " + sampleDataSize);

        /**
         *
         * takeSample allow the user takeSample(withReplacement, num, [seed]) to return an array with
         * a random sample of num elements of the dataset, with or without replacement, optionally
         * pre-specifying a random number generator seed.
         *
         */

        List<String> sampledDataList = rawData.takeSample(false, 100, 20);
        System.out.println(rawDataSize + " and after the sampling: " + sampledDataList.size());



    }

    private static File downloadFile(String downloadUrl, String outFileName) throws IOException {
        File outputFile = null;
        DefaultHttpClient httpClient = new DefaultHttpClient();
        HttpGet httpGet = new HttpGet(downloadUrl);
        org.apache.http.HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            outputFile = new File(outFileName);
            InputStream inputStream = entity.getContent();
            FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
            int read = 0;
            byte[] bytes = new byte[1024];
            while ((read = inputStream.read(bytes)) != -1) {
                fileOutputStream.write(bytes, 0, read);
            }
            fileOutputStream.close();
        }else {
            System.out.println("Download failed!");
        }
        return outputFile;
    }
}
