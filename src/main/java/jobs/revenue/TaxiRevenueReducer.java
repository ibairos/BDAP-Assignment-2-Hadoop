package jobs.revenue;

import model.writable.YearMonthPair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TaxiRevenueReducer extends Reducer<YearMonthPair, DoubleWritable, YearMonthPair, DoubleWritable> {

    @Override
    public void reduce(YearMonthPair key, Iterable<DoubleWritable> values, Context context) throws IOException,
            InterruptedException {
        double revenue = 0;

        for (DoubleWritable d : values) {
            revenue += d.get();
        }
        context.write(key, new DoubleWritable(revenue));
    }
}

