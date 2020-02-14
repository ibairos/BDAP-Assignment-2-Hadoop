package model.writable;

import com.google.gson.Gson;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaxiIDPair implements Writable, WritableComparable<TaxiIDPair> {

    // First and second key
    private Text taxiID;
    private Text time;

    public TaxiIDPair() {
        super();
        taxiID = new Text();
        time = new Text();
    }

    public TaxiIDPair(Text taxiID, Text time) {
        super();
        this.taxiID = taxiID;
        this.time = time;
    }

    public Text getTaxiID() {
        return taxiID;
    }

    public void setTaxiID(Text taxiID) {
        this.taxiID = taxiID;
    }

    public Text getTime() {
        return time;
    }

    public void setTime(Text time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    public int compareTo(TaxiIDPair o) {
        int comp = taxiID.compareTo(o.getTaxiID());
        return comp != 0 ? comp : time.compareTo(o.getTime());
    }

    public void write(DataOutput dataOutput) throws IOException {
        taxiID.write(dataOutput);
        time.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        taxiID.readFields(dataInput);
        time.readFields(dataInput);
    }
}
