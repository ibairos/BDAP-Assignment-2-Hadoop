package jobs;

import model.writable.Segment;
import model.writable.TaxiIDPair;
import org.apache.hadoop.mapreduce.Partitioner;

public class TaxiPartitioner extends Partitioner<TaxiIDPair, Segment> {

    @Override
    public int getPartition(TaxiIDPair taxiIDPair, Segment segment, int partitionNum) {
        return taxiIDPair.getTaxiID().hashCode() % partitionNum;
    }
}