package model.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearMonthPair implements Writable, WritableComparable<YearMonthPair> {

    private IntWritable year;
    private IntWritable month;

    public YearMonthPair() {
        super();
        year = new IntWritable();
        month = new IntWritable();
    }

    public YearMonthPair(int year, int month) {
        super();
        this.year = new IntWritable(year);
        this.month = new IntWritable(month);
    }

    public IntWritable getYear() {
        return year;
    }

    public IntWritable getMonth() {
        return month;
    }

    @Override
    public String toString() {
        return year.toString() + "\t" + month.toString();
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        year.write(dataOutput);
        month.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year.readFields(dataInput);
        month.readFields(dataInput);
    }

    @Override
    public int compareTo(YearMonthPair o) {
        int year1 = year.get();
        int month1 = month.get();
        int year2 = o.getYear().get();
        int month2 = o.getMonth().get();

        if (year1 == year2) {
            if (month1 == month2) {
                return 0;
            } else {
                return month1 >= month2 ? 1 : -1;
            }
        } else {
            return year1 >= year2 ? 1 : -1;
        }

    }
}
