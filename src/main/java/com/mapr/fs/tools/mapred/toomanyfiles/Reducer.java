package com.mapr.fs.tools.mapred.toomanyfiles;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Reducer extends MapReduceBase
    implements org.apache.hadoop.mapred.Reducer<LongWritable, Text, Text, Text> {
  boolean first = true;

  @Override
  public void reduce(LongWritable key, Iterator<Text> values,
                     OutputCollector<Text, Text> output,
                     Reporter reporter) throws IOException {
    if (first) {
      first = false;
      output.collect(
          new Text("Time (sec)"),
          new Text("count\tmin_c\tavg_c\tmax_c\tmin_w\tavg_w\tmax_w\t"));
    }

    int count = 0;
    int min_create = Integer.MAX_VALUE;
    int max_create = Integer.MIN_VALUE;
    long sum_create = 0;

    int min_write = Integer.MAX_VALUE;
    int max_write = Integer.MIN_VALUE;
    long sum_write = 0;

    while (values.hasNext()) {
      count++;
      String[] value = values.next().toString().split("\t");
      int create = Integer.valueOf(value[0]);
      if (create < min_create) min_create = create;
      if (create > max_create) max_create = create;
      sum_create += create;

      int write = Integer.valueOf(value[1]);
      if (write < min_write) min_write = write;
      if (write > max_write) max_write = write;
      sum_write += write;
    }

    output.collect(new Text(String.valueOf(key.get())),
                   new Text(String.format("%d\t%d\t%d\t%d\t%d\t%d\t%d\t", count,
                            min_create, sum_create/count, max_create,
                            min_write, sum_write/count, max_write)));
  }

  @Override
  public void configure(JobConf job) {
    // TODO Auto-generated method stub
    super.configure(job);
  }

}