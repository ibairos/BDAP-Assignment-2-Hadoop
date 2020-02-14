package jobs.reconstruct;

import model.TaxiStatus;
import model.writable.Segment;
import model.writable.TaxiIDPair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TaxiReconstructReducer extends Reducer<TaxiIDPair, Segment, NullWritable, Segment> {

    @Override
    protected void reduce(TaxiIDPair key, Iterable<Segment> segments, Context context) throws IOException,
            InterruptedException {

        Segment mergedSegment = null;

        for (Segment s : segments) {
            if (s.getStartStatus() == TaxiStatus.EMPTY && s.getEndStatus() == TaxiStatus.FULL) {
                mergedSegment = s.getCopy();
            } else if (s.getStartStatus() == TaxiStatus.FULL && s.getEndStatus() == TaxiStatus.FULL) {
                if (mergedSegment != null && mergedSegment.isNextSegment(s.getStartTimeMillis())) {
                    mergedSegment.merge(s);
                }
            } else if (s.getStartStatus() == TaxiStatus.FULL && s.getEndStatus() == TaxiStatus.EMPTY) {

                if (mergedSegment != null && mergedSegment.isNextSegment(s.getStartTimeMillis())) {
                    mergedSegment.merge(s);
                    if (mergedSegment.isTaxiInAirport()) {
                        context.write(NullWritable.get(), mergedSegment);
                    }
                }
                mergedSegment = null;
            }
        }
    }
}
