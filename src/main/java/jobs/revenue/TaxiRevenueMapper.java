package jobs.revenue;

import com.google.gson.Gson;
import common.Common;
import model.writable.Segment;
import model.writable.YearMonthPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Calendar;

public class TaxiRevenueMapper extends Mapper<Object, Text, YearMonthPair, Segment> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Segment trip = new Gson().fromJson(value.toString(), Segment.class);
        Common.CALENDAR.setTimeInMillis(trip.getStartTimeMillis());

        if (trip.isTaxiInAirport()) {
            context.write(new YearMonthPair(Common.CALENDAR.get(Calendar.YEAR), Common.CALENDAR.get(Calendar.MONTH)), trip);
        }

    }
}
