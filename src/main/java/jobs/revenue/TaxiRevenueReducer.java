package jobs.revenue;

import model.writable.Segment;
import model.writable.YearMonthPair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TaxiRevenueReducer extends Reducer<YearMonthPair, Segment, YearMonthPair, DoubleWritable> {

    @Override
    public void reduce(YearMonthPair key, Iterable<Segment> values, Context context) throws IOException,
            InterruptedException {
        double revenue = 0;

        for (Segment trip : values) {
            revenue += trip.getRevenue();
        }
        context.write(key, new DoubleWritable(revenue));
    }
}

