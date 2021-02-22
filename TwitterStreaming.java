import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.util.Arrays;
import java.util.List;

public class TwitterStreaming {
    public static void main(String[] args) {
        System.out.println("Start:");
        Logger.getLogger("org.apache").setLevel(Level.ERROR);
        SparkConf config = new SparkConf().setMaster("local[2]").setAppName("Assignment5");

//  put all the credentials
        System.setProperty("twitter4j.oauth.consumerKey", "");
        System.setProperty("twitter4j.oauth.consumerSecret", "");
        System.setProperty("twitter4j.oauth.accessToken", "");
        System.setProperty("twitter4j.oauth.accessTokenSecret", "");

//  create JavaStreamingContext
        JavaStreamingContext jsscs = new JavaStreamingContext(config, new Duration(5000));
        JavaDStream<Status> javaReceiverInputDStream = TwitterUtils.createStream(jsscs).filter(status -> status.getLang().equals("en"));

//  stream the tweets - the below function takes the input stream and get texts for all tweets and stores it in JavaDStream names -> tweets
        JavaDStream<String> tweets = javaReceiverInputDStream.map(
                new Function<Status, String>() {
                    @Override
                    public String call(Status status) throws Exception {
                        return status.getText();
                    }
                });

//  split the tweets with space to get the words - on the bases of space regex. and create a JavaDStream named -> wordSplit
        JavaDStream<String> wordSplit = tweets.flatMap(in -> {
            List<String> words = Arrays.asList(in.split(" "));
            return words.iterator();
        });

//  count the words in wordSplit, so it is the total number of words in the stream
        wordSplit.foreachRDD(r -> {
            System.out.println("total number of words:" + r.count());
        });

//  split the words to get the characters - on the bases of regex, and create a JavaDStream named -> characterSplit
        JavaDStream<String> characterSplit = wordSplit.flatMap(in -> {
            List<String> character = Arrays.asList(in.split(""));
            return character.iterator();
        });

//  count the characters characterSplit, so it is the total number of character in the stream
        characterSplit.foreachRDD(r -> {
            System.out.println("total number of character:" + r.count());
        });

//  extract the hashtags, which start with a # symbol
        JavaDStream<String> hashTags = wordSplit.filter(
                new Function<String, Boolean>() {
                    public Boolean call(String word) {
                        return word.startsWith("#");
                    }
                }
        );

//  print the hashtags, which are present in JavaDStream named -> hashtags
        hashTags.foreachRDD(r -> {
            System.out.println("total number of Hashtags:" + r.count());
            System.out.println("HashTags:");
            r.collect().forEach(System.out::println);
            System.out.println("\n");
        });


//  window operations for window of 5 minutes, and stream every 30 seconds
        JavaDStream<String> windowTweets = tweets.window(Durations.minutes(5), Durations.seconds(30));
//  split the tweets in the window
        JavaDStream<String> wordSplitWindows = windowTweets.flatMap(in -> {
            List<String> words = Arrays.asList(in.split(" "));
            return words.iterator();
        });
//  count the average number of tweets in the window
        windowTweets.foreachRDD(rdd -> {
//          total tweets in the window
            long ttiw = rdd.count();
//          create a JavaRDD for the length of each tweet
            JavaRDD<Integer> eachTweetLength = rdd.map(s -> s.split(" ").length);
//          reduce the JavaRDD to the total length
            int totalLength = eachTweetLength.reduce((a, b) -> a + b);
//          print the average
            System.out.println("Average word count in window:" + (totalLength / ttiw));
        });

//      extract the hashtags for windows
        JavaDStream<String> hashTagsWindows = wordSplitWindows.filter(
                (Function<String, Boolean>) word -> word.startsWith("#")
        );
//      create the hashtag and associated its value that is 1
        JavaPairDStream<String, Integer> pairs = hashTagsWindows.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<String, Integer>(s, 1));
//      reduce the total hashtags to count each
        JavaPairDStream<String, Integer> wordCountN = pairs.reduceByKey(
                (Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);
//      swap the pair of hashtag and value because we need to sort it and the key need to the first parameter
        JavaPairDStream<Integer, String> swap = wordCountN.mapToPair(x -> x.swap());
//      now we can sort the the swapped key value pair, False in the sortByKey is to reverse sort it means decreasing order
        JavaPairDStream<Integer, String> sorted = swap.transformToPair(in -> in.sortByKey(false));
//      now we can again sort to get the original position. this is optional
        JavaPairDStream<String, Integer> swap_again = sorted.mapToPair(x -> x.swap());
//      print the top 10 most hashtags
        swap_again.print(10);


//      start the jsscs
        jsscs.start();
        try {
            jsscs.awaitTermination();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("End");
    }

}

