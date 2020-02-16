package jobs.reconstruct;

import model.TripStatus;
import model.writable.Segment;
import model.writable.TaxiIDPair;
import model.writable.Trip;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TaxiReconstructReducer extends Reducer<TaxiIDPair, Segment, NullWritable, Trip> {

    @Override
    protected void reduce(TaxiIDPair key, Iterable<Segment> segments, Context context) throws IOException,
            InterruptedException {

        Segment mergedSegment = null;

        for (Segment s : segments) {
            if (s.getStatus() == TripStatus.START) {
                mergedSegment = s.getCopy();
            } else if (s.getStatus() == TripStatus.MIDDLE) {
                if (mergedSegment != null && mergedSegment.isNextSegment(s)) {
                    mergedSegment.merge(s);
                }
            } else if (s.getStatus() == TripStatus.END) {
                if (mergedSegment != null && mergedSegment.isNextSegment(s)) {
                    mergedSegment.merge(s);
                    if (mergedSegment.isTaxiInAirport()) {
                        context.write(NullWritable.get(), new Trip(mergedSegment.getStartTimeMillis(),
                                mergedSegment.getDistance()));
                    }
                }
                mergedSegment = null;
            }
        }
    }

}
