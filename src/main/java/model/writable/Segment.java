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

    private double distance;

    private boolean taxiInAirport;

    public Segment() {
        super();
    }

    public Segment(long startTimeMillis, long endTimeMillis, TripStatus status, double distance,
                   boolean taxiInAirport) {
        this.startTimeMillis = startTimeMillis;
        this.endTimeMillis = endTimeMillis;
        this.status = status;
        this.distance = distance;
        this.taxiInAirport = taxiInAirport;
    }

    public Segment(double startLat, double startLong, double endLat, double endLong, long startTimeMillis,
                   long endTimeMillis, TripStatus status) {
        this.startTimeMillis = startTimeMillis;
        this.endTimeMillis = endTimeMillis;
        this.status = status;
        distance = DistanceUtils.flatSurfaceDistance(startLat, startLong, endLat, endLong);
        taxiInAirport = DistanceUtils.taxiInAirport(startLat, startLong, endLat, endLong);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(startTimeMillis);
        dataOutput.writeLong(endTimeMillis);
        dataOutput.writeUTF(status.toString());
        dataOutput.writeDouble(distance);
        dataOutput.writeBoolean(taxiInAirport);
    }

    public void readFields(DataInput dataInput) throws IOException {
        startTimeMillis = dataInput.readLong();
        endTimeMillis = dataInput.readLong();
        status = TripStatus.valueOf(dataInput.readUTF());
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

    public boolean isNextSegment(long newSegmentStartTime, TripStatus nextStatus) {
        boolean correctStatus =
                (
                        nextStatus == TripStatus.MIDDLE &&
                        (status == TripStatus.START || status == TripStatus.MIDDLE)
                )
                ||
                (
                        nextStatus == TripStatus.END &&
                        (status == TripStatus.MIDDLE)
                );

        return endTimeMillis <= newSegmentStartTime
                && newSegmentStartTime < endTimeMillis + Common.SEGMENT_TOLERANCE_MILLIS
                && correctStatus;
    }

    public boolean isValid() {
        return status != TripStatus.NULL;
    }

    public void merge(Segment segment) {
        endTimeMillis = segment.getEndTimeMillis();
        status = segment.getStatus();
        distance += segment.getDistance();
        taxiInAirport = taxiInAirport || segment.isTaxiInAirport();
    }

    public Segment getCopy() {
        return new Segment(startTimeMillis, endTimeMillis, status, distance, taxiInAirport);
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
