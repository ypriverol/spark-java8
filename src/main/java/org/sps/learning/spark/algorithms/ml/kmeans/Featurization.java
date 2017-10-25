package org.sps.learning.spark.algorithms.ml.kmeans;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.sps.learning.spark.utils.SparkUtil;
import scala.Tuple2;

import java.util.Calendar;
import java.util.Date;
import java.util.stream.IntStream;

/**
 * A standalone Spark Java program to perform featurization of wikistats 
 * data defined below. 
 * 
 * The Scala version of featurization program is given here: 
 *   http://ampcamp.berkeley.edu/exercises-strata-conf-2013/featurization.html
 * 
 * 
 * Data to download: http://dumps.wikimedia.org/other/pagecounts-raw/
 * 
 *  Each record in our dataset (input files) consists of a string with the format 
 *  “<date_time> <project_code> <page_title> <num_hits> <page_size>”. 
 *  Note that the format of the "<date-time>" field is YYYYMMDD-HHmmSS
 *  (where ‘MM’ denotes month, and ‘mm’ denotes minute). Note the downloaded
 *  data will not have the <date_time> field, so we need to append it from 
 *  the filenames.  Please see my notes below.
 * 
 * 
 * The first few lines of the file are copied here:
 *
 *    aa ?page=http://www.stockphotosharing.com/Themes/Images/users_raw/id.txt 3 39267
 *    aa Main_Page 7 51309
 *    aa Special:Boardvote 1 11631
 *    aa Special:Imagelist 1 931
 *    aa.b ?71G4Bo1cAdWyg 1 14463
 *    aa.b Special:Statistics 1 840
 *    aa.b Special:Whatlinkshere/MediaWiki:Returnto 1 1019
 * 
 * The goal of this class is to convert each record of 5 items:
 * “<project_code> <page_title> <num_hits> <page_size>”
 * into 24 features (described below).
 * 
 * Note that the data in http://dumps.wikimedia.org/other/pagecounts-raw/ does not 
 * have the <date-time> field.  But, you may add/insert the <date-time> field from 
 * the filename: for example:  for the downloaded file: 
 *    http://dumps.wikimedia.org/other/pagecounts-raw/2015/2015-11/pagecounts-20151101-000000.gz
 * we have (after un-zipping):
 * 
 *  # tail -3 pagecounts-20151101-000000
 *  zu.d uwoyela 1 4386
 *  zu.d wildebeest 1 5965
 *  zu.mw zu 28 180647 
 * 
 * For this data (pagecounts-20151101-000000), you need to generate (append <date-time> 
 * from the filename for each record):
 *  20151101-000000 zu.d uwoyela 1 4386
 *  20151101-000000 zu.d wildebeest 1 5965
 *  20151101-000000 zu.mw zu 28 180647 
 * 
 *
 * @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 * 
 *
 */          
public class Featurization {
    
    private static final Logger LOGGER = Logger.getLogger(Featurization.class);
    private static final String WIKI_DATA_FOLDER = "./data/wikidata/";
    private static final String WIKI_FATURED_FOLDER = "./hdfs/wikidata/featurized/";
   
    public static void main(String[] args) throws Exception {
        
        // In this section, we will walk you through the steps 
        // to preprocess and featurize the Wikipedia dataset.

        final String wikistatsPath = WIKI_DATA_FOLDER;  // /data/wikistats
        LOGGER.info("output path="+ wikistatsPath);

        // input record: “<project_code> <page_title> <num_hits> <page_size>”

        final String outputPath = WIKI_FATURED_FOLDER; // for featurized data: /data/wikistats_featurized
        // output record: “(K, V)
        // where 
        //   K: <project_code> + " " + <page_title>”
        //   V: <feature_1><,><feature_2><,>...<,><feature_24> (one feature value per hour)
        LOGGER.info("output path="+outputPath);

        
        // create a JavaSparkContext, used to create RDDs
        JavaSparkContext context = SparkUtil.createJavaSparkContext("Wiki Featured", "local[2]");
        
        //
        // read input data and create the first RDD
        // In recent implementations spark has enable to read all gz in an specific folder
        //
        JavaRDD<String> wikistatsRDD = context.textFile(wikistatsPath + "*.gz");
        
        // Next, for every line of data, we collect a tuple with elements described next.
        //
        //          The first element is what we will call the “full document title”, 
        //          a concatenation of the project code and page title. 
        //
        //          The second element is a key-value pair whose key is the hour from 
        //          the <date-time> field and whose value is the number of views that 
        //          occurred in this hour.
        //
        // There are a few new points to note about the code below. First, data.map takes each 
        // line of data in the RDD data and applies the function passed to it. The first step 
        // splits the line of data into the five data fields we discussed in the Spark exercises 
        // above. The second step extracts the hour information from the <date-time> string and 
        // we then form the output tuple.  
        //
        // scala:
        //    val featureMap = data.map(line => {
        //      val Array(dateTime, projectCode, pageTitle, numViews, numBytes) = line.trim.split("\\s+")
        //      val hour = dateTime.substring(9, 11).toInt
        //      (projectCode+" "+pageTitle, hour -> numViews.toInt)
        //    })
        //
        // featureMap = Tuple2<projectCode+" "+pageTitle, Tuple2<hour, NumViews>>
        JavaPairRDD<String, Tuple2<Integer,Integer>> featureMap = wikistatsRDD.mapToPair(
                (PairFunction<String, String, Tuple2<Integer, Integer>>) rec -> {
                    // rec =     projectCode, pageTitle, numViews, numBytes
                    // tokens[]      0           1           2        3
                    String[] tokens = StringUtils.split(rec, " ");

                    Calendar rightNow = Calendar.getInstance();
                    int hour = rightNow.get(Calendar.HOUR_OF_DAY);

                    String projectCode = tokens[0].trim();
                    String pageTitle = tokens[1].trim();
                    int numViews = Integer.parseInt(tokens[2].trim());
                    String K = projectCode + " " + pageTitle;
                    Tuple2<Integer,Integer> V = new Tuple2(hour,numViews);
                    return new Tuple2(K, V);
                });
        
        
        // Now we want to find the average hourly views for each article (average for the same 
        // hour across different days).  In the code below, we first take our tuples in the RDD 
        // featureMap and, treating the first elements (i.e., article name) as keys and the second 
        // elements (i.e., hoursViewed) as values, group all the values for a single key (i.e., a 
        // single article) together using groupByKey. We put the article name in a variable called 
        // article and the multiple tuples of hours and pageviews associated with the current 
        // article in a variable called hoursViews. The for loop then collects the number of days 
        // for which we have a particular hour of data in counts[hour] and the total pageviews at 
        // hour across all these days in sums[hour]. Finally, we use the syntax sums zip counts to 
        // make an array of tuples with parallel elements from the sums and counts arrays and use 
        // this to calculate the average pageviews at particular hours across days in the data set. 
        // scala:
        //      val featureGroup = featureMap.groupByKey.map(grouped => {
        //      val (article, hoursViews) = grouped
        //      val sums = Array.fill[Int](24)(0)
        //      val counts = Array.fill[Int](24)(0)
        //      for((hour, numViews) <- hoursViews) {
        //          counts(hour) += 1
        //          sums(hour) += numViews
        //      }
        //      val avgs: Array[Double] =
        //      for((sum, count) <- sums zip counts) yield
        //          if(count > 0) sum/count.toDouble else 0.0
        //          article -> avgs
        //      })        
        JavaPairRDD<String, Iterable<Tuple2<Integer,Integer>>> featureMapGrouped = featureMap.groupByKey();
        JavaPairRDD<String, double[]> featureGroup = featureMapGrouped.mapValues(
                (Function<Iterable<Tuple2<Integer, Integer>>, double[]>) rs -> {
                    //
                    double[] sums = new double[24];
                    double[] counts = new double[24];
                    rs.forEach( pair -> {
                        int hour = pair._1;
                        int numViews = pair._2;
                        counts[hour] += 1.0;
                        sums[hour] += numViews;
                    });
                    double[] avgs = new double[24];
                    IntStream.range(0, 10).forEach(
                            i -> {
                                if (counts[i] == 0) {
                                    avgs[i] = 0.0;
                                }
                                else {
                                    avgs[i] = sums[i] / counts[i];
                                }
                            }
                    );
                    return avgs;
                });
             
 
        // Now suppose we’re only interested in those articles that were viewed 
        // at least once in each hour during the data collection time.
        // To do this, we filter to find those articles with an average number of 
        // views (the second tuple element in an article tuple) greater than zero 
        // in every hour.       
        //
        // scala:
        //  val featureGroupFiltered = featureGroup.filter(t => t._2.forall(_ > 0))  
        //
        JavaPairRDD<String, double[]> featureGroupFiltered = featureGroup.filter(
                (Function<Tuple2<String, double[]>, Boolean>) s -> {
                    int nonZero = 0;
                    for (double d : s._2) {
                        if (d > 0.0)
                            nonZero++;
                    }
                    //
                    // keep these records
                    return nonZero == 24;
                });
        
        
        
        // So far article popularity is still implicitly in our feature vector 
        // (the sum of the average views per hour is the average views per day 
        // if the number of days of data is constant across hours). Since we are 
        // interested only in which times are more popular viewing times for each 
        // article, we next divide out by this sum.
        //
        // Scala:
        //      val featurizedRDD = featureGroupFiltered.map(t => {
        //          val avgsTotal = t._2.sum
        //          t._1 -> t._2.map(_ /avgsTotal)
        //      })      
        JavaPairRDD<String, double[]> featurizedRDD = featureGroupFiltered.mapValues(
                (Function<double[], double[]>) data -> {
                    //
                    final double[] sum = {0.0};
                    IntStream.range(0, 24).forEach( i ->  {
                        sum[0] += data[i];
                    });
                    //
                    double[] avg = new double[24];
                    IntStream.range(0, 24).forEach( i -> {
                        avg[i] = data[i]/ sum[0];
                    });
                    return avg;
                });
        
        // Count the number of records in the preprocessed data. Recall that we 
        // potentially threw away some data when we filtered out records with 
        // zero views in a given hour.        
        long count = featurizedRDD.count();
        LOGGER.info("count="+count);
        
         
        // Finally, we can save the RDD to a file for later use. 
        // To save our features to a file, we first create a string of 
        // comma-separated values for each data point and then save it 
        // in HDFS as file named wikistats_featurized.
        //
        //  Scala:
        //      featurizedRDD.cache.map(t => t._1 + "#" + t._2.mkString(","))
        //
        JavaRDD<String> finalFeaturizedRDD = buildFeaturizedOutput(featurizedRDD);
        
        //
        // save the final output for future use
        //
        finalFeaturizedRDD.saveAsTextFile(outputPath);
        
        // done
        context.close();
    }
    
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
    private static JavaRDD<String> buildFeaturizedOutput(final JavaPairRDD<String, double[]> featurizedRDD) {

        return featurizedRDD.map(
                (Function<Tuple2<String, double[]>, String>) kv -> {
                    StringBuilder builder = new StringBuilder();
                    //
                    builder.append(kv._1); // key
                    builder.append("#");   // separator of key from the values/features
                    //
                    double[] data = kv._2;
                    for (int i=0; i < 23; i++) {
                       builder.append(data[i]); // feature
                       builder.append(",");
                    }
                    builder.append(data[23]); // the last feature (24'th)
                    //
                    return builder.toString();
                });
    }
       
}
