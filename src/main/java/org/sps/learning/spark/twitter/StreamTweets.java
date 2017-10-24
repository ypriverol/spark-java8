package org.sps.learning.spark.twitter;

import com.google.common.io.Files;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import org.sps.learning.spark.twitter.data.Tweet;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.nio.charset.Charset;

public class StreamTweets {

    public static void main(String[] args) throws Exception {
        // Twitter4J
        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);

        ObjectMapper mapper = new ObjectMapper();

        // Set up SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("Stream Tweets").setMaster("local[2]")
                .set("spark.serializer", KryoSerializer.class.getName())
                .set("es.index.auto.create", "true");

        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(5000));

        //DataFrame for old tweets
        JavaSparkContext jsc = sc.sparkContext();

        // output file, save streamed twitter data as a json file
        final File outputFile = new File("data/tweets.json");

        //create a DStream of tweets
        String[] filters = { "@mattsinthemkt", "@IvarsClam", "@pikeplchowder", "@BaccoCafeSea", "@RadiatorWhiskey",
                                "@CuttersCrab", "@japonessa", "@PinkDoorSeattle", "@CrepedeFrance1", "@place_pigalle",
                                "Kastoori Grill", "kastoori grill", "@canlis", "@MetGrill", "@thewalrusbar",
                                "@dahlialounge", "@RN74Seattle", "@ElGauchoSteak", "@Spinasse", "@ILBistroSeattle",
                                "@PalisadeSea", "@SaltySeattle", "@Andaluca", "@RockCreekSea", "@salareseattle",
                                "@AOTTSeattle", "@AlturaSeattle", "@GoldfinchTavern", "@raysboathouse", "@SeriousPieDT",
                                "@SeriousPiePike", "@ilcorvopasta", "@ElliottsSeattle", "@mamnoontoo", "@DelanceySeattle",
                                "@WildGingerEats", "@TheCarlileRoom", "@NellsRestaurant", "@steelheaddiner", "@EatStoneburner",
                                "@SandPointGrill1", "@LOWELLSBar", "@ChandlersCrab", "@Local360Seattle", "@curb_cuisine",
                                "@DukesChowder", "@PiattiSeattle", "@petitcochonsea", "@brimmerheeltap", "@rione_xiii",
                                "@BrunswickHunt", "@coastalkitchen", "@bramlingballard", "@13CoinsSeattle", "@blueacreseafood",
                                "@CafeMunir", "@SeatownSeabar", "@oldstovebeer", "@pikebrewing", "@SeaCoffeeWorks",
                                "@BeechersSeattle", "@CanonSeattle", "@TheVirginiaInn", "@biscuitbitch"};


        //get twitter stream as a string with only english tweets and containing filters
        JavaDStream<String> stream = TwitterUtils.createStream(sc, twitterAuth, filters)
                .map(s -> new Tweet(s.getUser().getName(), s.getText(), s.getCreatedAt(), s.getPlace(), s.getGeoLocation(), s.getLang(), null))
                .map(mapper::writeValueAsString)
                .filter(t -> t.contains("\"language\":\"en\""));


        // save stream to json file
        stream.foreachRDD(tweetsRDD -> {
            tweetsRDD.collect().stream().forEach(System.out::println);
            tweetsRDD.foreach(t ->
                    Files.append(t + "\n", outputFile, Charset.defaultCharset())
            );
        });

        sc.start();
        sc.awaitTermination();
        sc.stop();

    }

}


