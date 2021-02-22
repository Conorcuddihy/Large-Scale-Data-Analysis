
import java.util.Arrays;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

public class ML {

	public static void main(String[] args) throws Exception {
//		This is the line to be kept on if there is no requirement of lots of Sparks logging messages in the console
//		Logger.getLogger("org.apache").setLevel(Level.ERROR);

		System.setProperty("hadoop.home.dir", "C:/winutils");
//		make the sparks configuration; set the amount of resources to be allocated
		SparkConf sparkConf = new SparkConf().setAppName("ML").setMaster("local[4]").set("spark.executor.memory", "2g");
//		set the sparks context
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//		read the text file given; which contains data
		JavaRDD<String> lines = ctx.textFile("..\\Weather_Spark\\imdb_labelled.txt", 1);
//		create a Hashing tf of the text data so that it can be used to train the SVM model 
		final HashingTF tf = new HashingTF();
//		in the variable named data we store the data in form of integer and second part which is sentence as the transformed array
		JavaRDD<LabeledPoint> data = lines.map(line -> {
			String[] tokens = line.split("\t");
			return new LabeledPoint(Integer.parseInt(tokens[1]), tf.transform(Arrays.asList(tokens[0].split(" "))));
		});

//here the data is splitted into the training and the testing; 60% data is for the training and remaining 40% is for testing 
//		training data
		JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 11L);
		training.cache();
//		testing data
		JavaRDD<LabeledPoint> test = data.subtract(training);

		// Run training algorithm to build the model.
		int numIterations = 1000;
		final SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);

		// Clear the default threshold.
		model.clearThreshold();

// I am doing two things
//			1. I have taken some sentences form the test set and manuly tries to check the accuracy of he model and also printed the predicted values and are correct.
		
		String negative_sentence_one = "A very, very, very slow-moving, aimless movie about a distressed, drifting young man";
		Vector negative_sentence_one_vector = tf.transform(Arrays.asList(negative_sentence_one.split(" ")));
		System.out.println("1. Prediction for negative test example: " + model.predict(negative_sentence_one_vector));

		String negative_sentence_two = "Not sure who was more lost - the flat characters or the audience, nearly half of whom walked out";
		Vector negative_sentence_two_vector = tf.transform(Arrays.asList(negative_sentence_two.split(" ")));
		System.out.println("2. Prediction for negative test example: " + model.predict(negative_sentence_two_vector));

		String positive_sentence_Three = "\"You'll love it! ";
		Vector positive_sentence_Three_vector = tf.transform(Arrays.asList(positive_sentence_Three.split(" ")));
		System.out.println("1. Prediction for positive test example: " + model.predict(positive_sentence_Three_vector));

		String positive_sentence_four = "A great film by a great director.";
		Vector positive_sentence_four_vector = tf.transform(Arrays.asList(positive_sentence_four.split(" ")));
		System.out.println("2. Prediction for positive test example: " + model.predict(positive_sentence_four_vector));

//			2. The second thing is i have taken the whole of the test data and have tried to print the prediced label plus the already present labels 
		test.foreach(f -> {
			double predicted = model.predict(f.features());
			System.out.println("Correct label as present in data file:"+f.label() + " Predicetd label:" + predicted);
		});

		
		// Compute raw scores on the test set.
		JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test
				.map(p -> new Tuple2<>(model.predict(p.features()), p.label()));

		// Get evaluation metrics.l;,,
		BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
		double auROC = metrics.areaUnderROC();
//		Here the Area under curve is printed as per the requirement 
		System.out.println("Area under ROC = " + auROC);

//	Here all the	
		ctx.stop();
		ctx.close();
	}
}