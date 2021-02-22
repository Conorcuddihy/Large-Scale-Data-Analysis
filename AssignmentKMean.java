import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;

import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;

public class AssignmentKMean {

	public static void main(String args[]) {
		System.setProperty("hadoop.home.dir", "C:/winutils");
//		make the sparks configuration; set the amount of resources to be allocated
		SparkConf sparkConf = new SparkConf().setAppName("ML").setMaster("local[4]").set("spark.executor.memory", "2g");
//		set the sparks context
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//		read the text file given; which contains data
		JavaRDD<String> rawData = ctx.textFile("..\\Twitter\\twit.txt", 1);

//		here we are making JavaRDD of string and vector. The string is the Tweet and the vector is the coordinate's
		JavaRDD<Tuple2<String, Vector>> featuredDataOne = rawData.map((String s) -> {
			String[] sarray = s.split(",");
			double[] values = new double[2];
			for (int i = 0; i < 2; i++)
				values[i] = Double.parseDouble(sarray[i]);
			return new Tuple2<String, Vector>(sarray[sarray.length - 1], Vectors.dense(values));
		});

		featuredDataOne.cache();

		// We are then clustering the data into 4 clusters on the bases of coordinate
		int numClusters = 4;
		int numIterations = 20;
		KMeansModel clusters = KMeans.train(featuredDataOne.map(t -> t._2).rdd(), numClusters, numIterations);

//		once the cluster is ready. we then predict the values and make a List of tweet's and predicted cluster and sort it
		List<Tuple2<String, Integer>> predictedCluster = featuredDataOne.map(f -> {
			return new Tuple2<String, Integer>(f._1, clusters.predict(f._2));
		}).sortBy(f->f._2, true,1).collect();

//		after we are ready with the tweet's and the predicted cluster sorted we now print it
		System.out.println("The sorted list:");
		predictedCluster.forEach(f -> {
			System.out.println(f._1 + "\t" + f._2);
		});
		
		ctx.stop();
		ctx.close();
	}
}
