package org.sps.learning.spark.twitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.twitter.*;
import org.apache.spark.streaming.api.java.*;
import twitter4j.*;

import java.io.File;
import java.util.Arrays;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

public class StreamTweets {

    public static JavaSparkContext sparkcontext;

    public static void main(String[] args) throws Exception {

        // Twitter4J
        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);

        ObjectMapper mapper = new ObjectMapper();

        // Set up SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("Stream Tweets").setMaster("local[2]");

        sparkcontext = new JavaSparkContext(sparkConf);
        JavaStreamingContext sc = new JavaStreamingContext(sparkcontext, new Duration(5000));

        // output file, save streamed twitter data as a json file
        final File outputFile = new File("./data/tweets.json");

        //create a DStream of tweets
//        String[] filters = { "@mattsinthemkt", "@IvarsClam", "@pikeplchowder", "@BaccoCafeSea", "@RadiatorWhiskey",
//                                "@CuttersCrab", "@japonessa", "@PinkDoorSeattle", "@CrepedeFrance1", "@place_pigalle",
//                                "Kastoori Grill", "kastoori grill", "@canlis", "@MetGrill", "@thewalrusbar",
//                                "@dahlialounge", "@RN74Seattle", "@ElGauchoSteak", "@Spinasse", "@ILBistroSeattle",
//                                "@PalisadeSea", "@SaltySeattle", "@Andaluca", "@RockCreekSea", "@salareseattle",
//                                "@AOTTSeattle", "@AlturaSeattle", "@GoldfinchTavern", "@raysboathouse", "@SeriousPieDT",
//                                "@SeriousPiePike", "@ilcorvopasta", "@ElliottsSeattle", "@mamnoontoo", "@DelanceySeattle",
//                                "@WildGingerEats", "@TheCarlileRoom", "@NellsRestaurant", "@steelheaddiner", "@EatStoneburner",
//                                "@SandPointGrill1", "@LOWELLSBar", "@ChandlersCrab", "@Local360Seattle", "@curb_cuisine",
//                                "@DukesChowder", "@PiattiSeattle", "@petitcochonsea", "@brimmerheeltap", "@rione_xiii",
//                                "@BrunswickHunt", "@coastalkitchen", "@bramlingballard", "@13CoinsSeattle", "@blueacreseafood",
//                                "@CafeMunir", "@SeatownSeabar", "@oldstovebeer", "@pikebrewing", "@SeaCoffeeWorks",
//                                "@BeechersSeattle", "@CanonSeattle", "@TheVirginiaInn", "@biscuitbitch"};

        String[] filters = {"@ypriverol"};


        // create Twitter stream
        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(sc, filters);

        // get output text of all tweets
        JavaDStream<String> statuses = twitterStream.map((Function<Status, String>) status -> status.getText());

        // split tweet stream into word stream (split a tweet into words)
        JavaDStream<String> words = statuses.flatMap(l -> Arrays.asList(l.split(" ")).iterator());

        // apply filter and get hashtags only
        JavaDStream<String> hashTags = words.filter((Function<String, Boolean>) word -> word.startsWith("#"));

        //hashTags.print();     // to print intermediate result

    /*
    *   1. Remove Hash from the words
    *   2. Lower case all words
    *   3. Convert Hashtags into Tuples of (<Hashtag>, 1)  format
    */
        JavaPairDStream<String, Integer> tuples = hashTags.mapToPair(l -> new Tuple2<>(l.substring(1).toLowerCase(), 1));

    /*
    *   1. Reduce by Key
    *   2. Perform sliding window operation
    */
        JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) { return i1 + i2; }
                },
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) { return i1 - i2; }
                },
                new Duration(60 * 5 * 1000),    /* Window Length */
                new Duration(60 * 5 * 1000)     /* Sliding Interval */
        );

        //    counts.print();     // check intermediate results

    /*
    *   1. Map function to swap the Tuple (Tuple<word, count> to Tuple<count, word>)
    */
        JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair(
                new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> in) {
                        return in.swap();
                    }
                }
        );

        // Apply transformation to sort by hashtags/key (by count)
        JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(
                new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
                    public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> in) throws Exception {
                        return in.sortByKey(false);
                    }
                }
        );


        // fetch the top 25 hashtags
        sortedCounts.foreachRDD( rdd -> {

            StringBuilder out = new StringBuilder("\nTop 25 hashtags:\n");
            for (Tuple2<Integer, String> t: rdd.take(25)) out.append(t.toString()).append("\n");
            System.out.println(out);       // print on the console
            // store top 25 hashtags on the file
            List<String> temp = new ArrayList<String>();
            temp.add(out.toString());
            JavaRDD<String> distData = sparkcontext.parallelize(temp);
            distData.saveAsTextFile("./data/tweets.txt");
        });

        sc.checkpoint("./hdfs/");
        sc.start();
        sc.awaitTermination();

    }

}


