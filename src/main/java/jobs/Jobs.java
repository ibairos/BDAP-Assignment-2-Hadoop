package jobs;

import jobs.reconstruct.TaxiReconstructMapper;
import jobs.reconstruct.TaxiReconstructReducer;
import jobs.revenue.TaxiRevenueMapper;
import jobs.revenue.TaxiRevenueReducer;
import launcher.ComputingRevenue;
import model.writable.Segment;
import model.writable.TaxiIDPair;
import model.writable.Trip;
import model.writable.YearMonthPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Jobs {

    private static final String RECONSTRUCTING_TRIPS = "ReconstructingTrips - Ibai";
    private static final String COMPUTING_REVENUE = "ComputingRevenue - Ibai";

    public static Job reconstructTripsJob(Path input, Path output) throws IOException {

        Job job = Job.getInstance(new Configuration(), RECONSTRUCTING_TRIPS);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(ComputingRevenue.class);

        job.setGroupingComparatorClass(TaxiComparator.class);
        job.setPartitionerClass(TaxiPartitioner.class);

        job.setMapperClass(TaxiReconstructMapper.class);
        job.setMapOutputKeyClass(TaxiIDPair.class);
        job.setMapOutputValueClass(Segment.class);

        job.setReducerClass(TaxiReconstructReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Trip.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(20);

        return job;
    }

    public static Job calculatingRevenueJob(Path input, Path output) throws IOException {

        Job job = Job.getInstance(new Configuration(), COMPUTING_REVENUE);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(ComputingRevenue.class);

        job.setMapperClass(TaxiRevenueMapper.class);
        job.setMapOutputKeyClass(YearMonthPair.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(TaxiRevenueReducer.class);
        job.setOutputKeyClass(YearMonthPair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}
