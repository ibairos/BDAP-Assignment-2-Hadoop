package jobs.reconstruct;

import common.Common;
import model.TripStatus;
import model.writable.Segment;
import model.writable.TaxiIDPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;

import static common.Common.DATE_FORMATTER;

public class TaxiReconstructMapper extends Mapper<Object, Text, TaxiIDPair, Segment> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] segmentArr = line.split(",");

        if (segmentArr.length == Common.SEGMENT_LENGTH) {
            try {
                String taxiID = segmentArr[0];
                long startTimeMillis = LocalDateTime.parse(segmentArr[1].replace("'", ""), DATE_FORMATTER)
                        .atZone(Common.ZONE_ID).toInstant().toEpochMilli();

                double startLat = Double.parseDouble(segmentArr[2]);
                double startLong = Double.parseDouble(segmentArr[3]);
                long endTimeMillis = LocalDateTime.parse(segmentArr[5].replace("'", ""), DATE_FORMATTER)
                        .atZone(Common.ZONE_ID).toInstant().toEpochMilli();
                double endLat = Double.parseDouble(segmentArr[6]);
                double endLong = Double.parseDouble(segmentArr[7]);
                TripStatus status = TripStatus.parse(segmentArr[4].replace("'", "").trim(),
                        segmentArr[8].replace("'", "").trim());

                Segment segment = new Segment(startLat, startLong, endLat, endLong, startTimeMillis, endTimeMillis,
                        status);
                if (segment.isValid() && !segment.exceedsMaxSpeed()) {
                    context.write(new TaxiIDPair(new Text(taxiID), new Text(String.valueOf(startTimeMillis))), segment);
                }
            } catch (Exception e) {
                System.out.println("MAP -> Value discarded: " + value);
            }
        }
    }
}
