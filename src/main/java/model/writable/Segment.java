package model.writable;

import com.google.gson.Gson;
import common.Common;
import common.DistanceUtils;
import model.TripStatus;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Segment implements Writable {

    private long startTimeMillis;
    private long endTimeMillis;

    private TripStatus status;

    private double startLat;
    private double startLong;
    private double endLat;
    private double endLong;

    private double distance;

    private boolean taxiInAirport;

    public Segment() {
        super();
    }

    public Segment(long startTimeMillis, long endTimeMillis, TripStatus status, double startLat, double startLong,
                   double endLat, double endLong, double distance, boolean taxiInAirport) {
        this.startTimeMillis = startTimeMillis;
        this.endTimeMillis = endTimeMillis;
        this.status = status;
        this.startLat = startLat;
        this.startLong = startLong;
        this.endLat = endLat;
        this.endLong = endLong;
        this.distance = distance;
        this.taxiInAirport = taxiInAirport;
    }

    public Segment(double startLat, double startLong, double endLat, double endLong, long startTimeMillis,
                   long endTimeMillis, TripStatus status) {
        this.startTimeMillis = startTimeMillis;
        this.endTimeMillis = endTimeMillis;
        this.status = status;
        this.startLat = startLat;
        this.startLong = startLong;
        this.endLat = endLat;
        this.endLong = endLong;
        distance = DistanceUtils.flatSurfaceDistance(startLat, startLong, endLat, endLong);
        taxiInAirport = DistanceUtils.taxiInAirport(startLat, startLong, endLat, endLong);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(startTimeMillis);
        dataOutput.writeLong(endTimeMillis);
        dataOutput.writeUTF(status.toString());
        dataOutput.writeDouble(startLat);
        dataOutput.writeDouble(startLong);
        dataOutput.writeDouble(endLat);
        dataOutput.writeDouble(endLong);
        dataOutput.writeDouble(distance);
        dataOutput.writeBoolean(taxiInAirport);
    }

    public void readFields(DataInput dataInput) throws IOException {
        startTimeMillis = dataInput.readLong();
        endTimeMillis = dataInput.readLong();
        status = TripStatus.valueOf(dataInput.readUTF());
        startLat = dataInput.readDouble();
        startLong = dataInput.readDouble();
        endLat = dataInput.readDouble();
        endLong = dataInput.readDouble();
        distance = dataInput.readDouble();
        taxiInAirport = dataInput.readBoolean();
    }


    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public void setStartTimeMillis(long startTimeMillis) {
        this.startTimeMillis = startTimeMillis;
    }

    public long getEndTimeMillis() {
        return endTimeMillis;
    }

    public void setEndTimeMillis(long endTimeMillis) {
        this.endTimeMillis = endTimeMillis;
    }

    public TripStatus getStatus() {
        return status;
    }

    public void setStatus(TripStatus status) {
        this.status = status;
    }

    public double getStartLat() {
        return startLat;
    }

    public void setStartLat(double startLat) {
        this.startLat = startLat;
    }

    public double getStartLong() {
        return startLong;
    }

    public void setStartLong(double startLong) {
        this.startLong = startLong;
    }

    public double getEndLat() {
        return endLat;
    }

    public void setEndLat(double endLat) {
        this.endLat = endLat;
    }

    public double getEndLong() {
        return endLong;
    }

    public void setEndLong(double endLong) {
        this.endLong = endLong;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public boolean isTaxiInAirport() {
        return taxiInAirport;
    }

    public void setTaxiInAirport(boolean taxiInAirport) {
        this.taxiInAirport = taxiInAirport;
    }

    public boolean exceedsMaxSpeed() {
        return distance / (((double) endTimeMillis - startTimeMillis) / (1000 * 3600)) > Common.MAX_SPEED_KPH;
    }

    public boolean distanceTooHigh() {
        return distance > Common.MAX_DISTANCE_KM;
    }

    public boolean isNextSegment(Segment segment) {
        boolean correctStatus =
                (
                        segment.getStatus() == TripStatus.MIDDLE &&
                        (status == TripStatus.START || status == TripStatus.MIDDLE)
                )
                ||
                (
                        segment.getStatus() == TripStatus.END &&
                        (status == TripStatus.MIDDLE)
                );

        boolean correctTime =
                endTimeMillis <= segment.getStartTimeMillis()
                && segment.getStartTimeMillis() - endTimeMillis < Common.SEGMENT_TOLERANCE_MILLIS;

        boolean correctLocation = DistanceUtils.flatSurfaceDistance(endLat, endLong, segment.getStartLat(),
                segment.getStartLong()) <= Common.DISTANCE_TOLERANCE_KM;

        return correctTime && correctStatus && correctLocation;
    }

    public boolean isValid() {
        return status != TripStatus.NULL;
    }

    public void merge(Segment segment) {
        endTimeMillis = segment.getEndTimeMillis();
        status = segment.getStatus();
        endLat = segment.endLat;
        endLong = segment.endLong;
        distance += segment.getDistance();
        taxiInAirport = taxiInAirport || segment.isTaxiInAirport();
    }

    public Segment getCopy() {
        return new Segment(startTimeMillis, endTimeMillis, status, startLat, startLong, endLat, endLong, distance,
                taxiInAirport);
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
