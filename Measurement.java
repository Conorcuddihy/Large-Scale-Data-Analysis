
public class Measurement {
	private int time;
	private double temperature;
	
	public Measurement(int t, double tem) {
		this.time=t;
		this.temperature=tem;
	}
	
	   //Setting up the setters and getters for the attributes of the object MeasurementQ1
    public void setTime(int time){
        this.time=time;
    }

    public int getTime(){
        return time;        
    }

    public void setTemperature(double temperature){
        this.temperature = temperature;
    }

    public double getTemperature(){
        return temperature;
    }
    @Override
    public String toString() {
        return this.getTime() + " " + this.getTemperature();
    }
}
