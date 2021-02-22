import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WeatherStation {

	private String cityLocation; // to store the city location
	private ArrayList<Measurement> measurements; // to store the array list of measurements
	private static ArrayList<WeatherStation> stations = new ArrayList<>(); // to store the array list of all the
																			// stations

	// constructor to assign city location and measurements
	public WeatherStation(String cityL, ArrayList<Measurement> measure) {
		this.cityLocation = cityL;
		this.measurements = measure;
	}

	// Setting up the setters and getters for the attributes of the object
	public void setCity(String city) {
		this.cityLocation = city;
	}

	public String getCity() {
		return cityLocation;
	}

	public void setMeasurements(ArrayList<Measurement> measurements) {
		this.measurements = measurements;
	}

	public ArrayList<Measurement> getMeasurements() {
		return measurements;
	}

	public static Tuple2<Double, Integer> countTemperature(double t1, double t2, double r) {
		System.setProperty("hadoop.home.dir", "C:/winutils");
		SparkConf sparkConf = new SparkConf().setAppName("Weather").setMaster("local[4]").set("spark.executor.memory","1g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		ArrayList<Double> allTemp = new ArrayList<Double>();
		stations.forEach(ws -> {
			ws.getMeasurements().forEach(m->{
				allTemp.add(m.getTemperature());
			});
		});

		JavaRDD<Double> distData = ctx.parallelize(allTemp);
		
		JavaPairRDD<Double, Integer> ones = distData.mapToPair((Double s) -> new Tuple2<Double, Integer>(s, 1));
		JavaPairRDD<Double, Integer> counts = ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);
		List<Tuple2<Double, Integer>> output = counts.collect();
			
		ctx.stop();
		ctx.close();
//		this is the list which is having the key value pair of the temperature and the count of the temperature between the specified range
		return output.get(0);
	}

	public static void main(String[] args) {
			
//		measurement objects are created
		Measurement m1 = new Measurement(1, 20);
		Measurement m2 = new Measurement(13, 11.7);
		Measurement m3 = new Measurement(15, -5.4);
		Measurement m4 = new Measurement(18, 18.7);
		Measurement m5 = new Measurement(20, 20.9);
//		added to array list
		ArrayList<Measurement> measure1 = new ArrayList<>();
		measure1.add(m1);
		measure1.add(m2);
		measure1.add(m3);
		measure1.add(m4);
		measure1.add(m5);

		Measurement m6 = new Measurement(3, 8.4);
		Measurement m7 = new Measurement(11, 19.2);
		Measurement m8 = new Measurement(28, 19.2);
		ArrayList<Measurement> measure2 = new ArrayList<>();
		measure2.add(m6);
		measure2.add(m7);
		measure2.add(m8);

//		weather station object is created and city and measurement is assigned to the object
		WeatherStation WS1 = new WeatherStation("City1", measure1);
		WeatherStation WS2 = new WeatherStation("City2", measure2);

//		weather station object is now added to the station arraylist
		stations.add(WS1);
		stations.add(WS2);
		System.out.println("List of temperature between the specified range: "+countTemperature(19.0, 10.8, 2.1));

	}
}
