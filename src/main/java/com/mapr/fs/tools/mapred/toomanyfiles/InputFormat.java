package com.mapr.fs.tools.mapred.toomanyfiles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class InputFormat implements org.apache.hadoop.mapred.InputFormat<Text, Text> {

  public static final String[] EMPTY_LOCATION = new String[0];

  public static class MyInputSplit implements InputSplit {
    int number;

    public MyInputSplit() {}

    public MyInputSplit(int number) {
      this.number = number;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(number);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      number = in.readInt();
    }

    @Override
    public String[] getLocations() throws IOException {
      return EMPTY_LOCATION;
    }

    @Override
    public long getLength() throws IOException {
      return 4;
    }
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    int num_writers = job.getInt("TestManyFiles.num_writers", 160);
    InputSplit[] splits = new InputSplit[num_writers];
    for (int i = 0; i < splits.length; i++) {
      splits[i] = new MyInputSplit(i);
    }
    return splits;
  }

  @Override
  public RecordReader<Text, Text> getRecordReader(final InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    return new RecordReader<Text, Text>() {
      boolean read = false;
      Text number = new Text(String.valueOf(((InputFormat.MyInputSplit) split).number));

      @Override
      public boolean next(Text key, Text value) throws IOException {
        if (read) {
          return false;
        } {
          return read = true;
        }
      }

      @Override
      public Text createKey() {
        return number;
      }

      @Override
      public Text createValue() {
        return number;
      }

      @Override
      public long getPos() throws IOException {
        return 0;
      }

      @Override
      public void close() throws IOException { }

      @Override
      public float getProgress() throws IOException {
        return read ? 1 : 0;
      }
    };
  }

}