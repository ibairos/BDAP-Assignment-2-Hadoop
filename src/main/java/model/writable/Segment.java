package model.writable;

import com.google.gson.Gson;
import common.Common;
import common.DistanceUtils;
import model.TaxiStatus;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Segment implements Writable {

    private long startTimeMillis;
    private long endTimeMillis;

    private TaxiStatus startStatus;
    private TaxiStatus endStatus;

    private double distance;

    private boolean taxiInAirport;

    public Segment() {
        super();
    }

    public Segment(long startTimeMillis, long endTimeMillis, TaxiStatus startStatus, TaxiStatus endStatus,
                   double distance, boolean taxiInAirport) {
        this.startTimeMillis = startTimeMillis;
        this.endTimeMillis = endTimeMillis;
        this.startStatus = startStatus;
        this.endStatus = endStatus;
        this.distance = distance;
        this.taxiInAirport = taxiInAirport;
    }

    public Segment(double startLat, double startLong, double endLat, double endLong, long startTimeMillis,
                   long endTimeMillis, TaxiStatus startStatus, TaxiStatus endStatus) {
        this.startTimeMillis = startTimeMillis;
        this.endTimeMillis = endTimeMillis;
        this.startStatus = startStatus;
        this.endStatus = endStatus;
        distance = DistanceUtils.flatSurfaceDistance(startLat, startLong, endLat, endLong);
        taxiInAirport = DistanceUtils.taxiInAirport(startLat, startLong, endLat, endLong);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(startTimeMillis);
        dataOutput.writeLong(endTimeMillis);
        dataOutput.writeUTF(startStatus.toString());
        dataOutput.writeUTF(endStatus.toString());
        dataOutput.writeDouble(distance);
        dataOutput.writeBoolean(taxiInAirport);
    }

    public void readFields(DataInput dataInput) throws IOException {
        startTimeMillis = dataInput.readLong();
        endTimeMillis = dataInput.readLong();
        startStatus = TaxiStatus.valueOf(dataInput.readUTF());
        endStatus = TaxiStatus.valueOf(dataInput.readUTF());
        distance = dataInput.readDouble();
        taxiInAirport = dataInput.readBoolean();
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

    public long getEndTimeMillis() {
        return endTimeMillis;
    }

    public void setEndTimeMillis(long endTimeMillis) {
        this.endTimeMillis = endTimeMillis;
    }

    public TaxiStatus getStartStatus() {
        return startStatus;
    }

    public void setStartStatus(TaxiStatus startStatus) {
        this.startStatus = startStatus;
    }

    public TaxiStatus getEndStatus() {
        return endStatus;
    }

    public void setEndStatus(TaxiStatus endStatus) {
        this.endStatus = endStatus;
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

    public boolean isNextSegment(long newSegmentStartTime) {
        return endTimeMillis <= newSegmentStartTime && newSegmentStartTime < endTimeMillis + Common.SEGMENT_TOLERANCE_MILLIS;
    }

    public boolean isFull() {
        return startStatus == TaxiStatus.FULL || endStatus == TaxiStatus.FULL;
    }

    public void merge(Segment segment) {
        endTimeMillis = segment.getEndTimeMillis();
        endStatus = segment.getEndStatus();
        distance += segment.getDistance();
        taxiInAirport = taxiInAirport || segment.isTaxiInAirport();
    }

    public Segment getCopy() {
        return new Segment(startTimeMillis, endTimeMillis, startStatus, endStatus, distance, taxiInAirport);
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
