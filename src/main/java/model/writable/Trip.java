package model.writable;

import com.google.gson.Gson;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Trip implements Writable {

    private long startTimeMillis;
    private double distance;

    public Trip() {
        super();
    }

    public Trip(long startTimeMillis, double distance) {
        super();
        this.startTimeMillis = startTimeMillis;
        this.distance = distance;
    }

    public double getRevenue() {
        return 3.5 + 1.71 * distance;
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public void setStartTimeMillis(long startTimeMillis) {
        this.startTimeMillis = startTimeMillis;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(startTimeMillis);
        dataOutput.writeDouble(distance);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        startTimeMillis = dataInput.readLong();
        distance = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
