package org.sps.learning.spark.twitter;


//https://www.tutorialspoint.com/spark_sql/spark_sql_useful_resources.htm
//https://spark.apache.org/docs/1.5.1/api/java/org/apache/spark/sql/DataFrame.html

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.sps.learning.spark.twitter.data.NaiveBayesKnowledgeBase;
import org.sps.learning.spark.twitter.model.NaiveBayes;

import java.io.File;
import java.io.IOException;


public class TwitterSentiment {

    static HashMap<String, Integer> map = new HashMap<>();

    public static void main(String[] args) throws IOException {

        SparkConf sparkConf = new SparkConf().setAppName("TwitterSentiment").setMaster("local[2]");
        SparkContext sc = new SparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(sc);

        //map of dataset files
        Map<String, File> trainingFiles = new HashMap<>();
        trainingFiles.put("Negative", new File("negative.txt"));
        trainingFiles.put("Positive", new File("positive.txt"));

        //loading examples in memory
        Map<String, String[]> trainingExamples = new HashMap<>();
        for(Map.Entry<String, File> entry : trainingFiles.entrySet()) {
            trainingExamples.put(entry.getKey(), readLines(entry.getValue()));
        }

        //train classifier
        NaiveBayes nb = new NaiveBayes();
        nb.setChisquareCriticalValue(4.94); //0.01 pvalue   //originally set at 6.63
        nb.train(trainingExamples);

        //get trained classifier knowledgeBase
        NaiveBayesKnowledgeBase knowledgeBase = nb.getKnowledgeBase();

        //Use classifier
        nb = new NaiveBayes(knowledgeBase);


        try {
            DataFrame tweets = sqlContext.read().json("restaurant.json"); // load old tweets into a DataFrame
            tweets.registerTempTable("tweetDF");

            DataFrame tweetText = sqlContext.sql("SELECT text FROM tweetDF");
            long numTweets = tweetText.count();
            System.out.println(numTweets);

            //go through all tweets and analyze the sentiment of each
            for(int i = 0; i<numTweets; i++) {

                String tweet = tweetText.take((int) numTweets)[i].toString();
                tweet = tweet.substring(1, tweet.length() - 1);
                System.out.println(tweet);

                Double sent = nb.predict(tweet);

                System.out.println("TwitterSentiment Prediction: " + sent);
            }

        } catch (Exception e){
            System.out.println(e);
        }

    }


    /**
     * Reads the all lines from a file and places it a String array. In each
     * record in the String array we store a training example text.
     *
     * @param file
     * @return
     * @throws IOException
     */

    public static String[] readLines(File file) throws IOException {

//        Scanner fileReader = new Scanner(file);
        List<String> lines;
        try (Scanner reader = new Scanner(file)) {
            lines = new ArrayList<>();
            String line = reader.nextLine();
            while (reader.hasNextLine()) {
                lines.add(line);
                line = reader.nextLine();
            }
        }
        return lines.toArray(new String[lines.size()]);
    }

}
