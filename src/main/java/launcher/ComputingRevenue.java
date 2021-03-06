package launcher;

import jobs.Jobs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;

public class ComputingRevenue {

    public static void main(String[] args) throws Exception {

        long startTime = System.nanoTime();

        Path input = new Path(args[0]);
        Path reconstructOut = new Path(args[1] + "/reconstruct");
        Path revenueOut = new Path(args[1] + "/revenue");

        deleteDirectory(new File(args[1]).toPath());

        Job reconstructJob = Jobs.reconstructTripsJob(input, reconstructOut);

        if (!reconstructJob.waitForCompletion(true)) {
            System.exit(1);
        }

        final long intermediateTime = System.nanoTime() - startTime;


        Job revenueJob = Jobs.calculatingRevenueJob(reconstructOut, revenueOut);
        if (!revenueJob.waitForCompletion(true)) {
            System.exit(2);
        }
        final long finalTime = System.nanoTime() - startTime;

        System.out.println("Reconstructing trips took " +
                String.format("%.3f", ((double) intermediateTime) / 1000000000) + " seconds.");
        System.out.println("Calculating revenue took " + String.format("%.3f",
                ((double) finalTime - intermediateTime) / 1000000000) + " seconds.");
        System.out.println("Whole program took " + String.format("%.3f", ((double) finalTime) / 1000000000) +
                " seconds.");

    }

    public static void deleteDirectory(java.nio.file.Path directory) {
        try {
            Files.walk(directory)
                    .sorted(Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(File::delete);
        } catch (IOException ignored) {

        }
    }

}
