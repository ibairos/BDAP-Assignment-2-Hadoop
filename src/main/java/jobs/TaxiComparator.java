package jobs;

import model.writable.TaxiIDPair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TaxiComparator extends WritableComparator {

    public TaxiComparator() {
        super(TaxiIDPair.class, true);
    }

    @Override
    public int compare(WritableComparable taxiId1, WritableComparable taxiId2) {
        return ((TaxiIDPair) taxiId1).getTaxiID().compareTo(((TaxiIDPair) taxiId2).getTaxiID());
    }
}
