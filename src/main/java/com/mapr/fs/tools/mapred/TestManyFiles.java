/* Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.fs.tools.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mapr.fs.tools.mapred.toomanyfiles.Mapper;
import com.mapr.fs.tools.mapred.toomanyfiles.InputFormat;
import com.mapr.fs.tools.mapred.toomanyfiles.Reducer;

public class TestManyFiles extends Configured implements Tool {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestManyFiles.class);

  private static final int DEFAULT_NUM_WRITERS = 240;

  public static final int DEFAULT_NUM_THREADS = 8;

  public static final int DEFAULT_FILE_COUNT = 2000;

  public static final int DEFAULT_FILE_SIZE = 2*1024*1024;

  private JobConf jobConf;

  public TestManyFiles(Configuration conf) {
    super(conf);
  }

  private static void usage() {
    System.err.println("Usage: TestManyFiles <output_dir> [options...]");
    System.err.println("Options:");
    System.err.println("  -DTestManyFiles.num_writers=<num_writers> (default: 240)");
    System.err.println("  -DTestManyFiles.num_threads=<num_threads_per_writer> (default: 8)");
    System.err.println("  -DTestManyFiles.file_count=<file_count_per_writer> (default: 2000)");
    System.err.println("  -DTestManyFiles.file_size=<file_size_per_file> (default: 2MB)");
    System.err.println("  -DTestManyFiles.run_reducer=<true|false> (default: true)");
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new TestManyFiles(new Configuration()), args);
    System.exit(ret);
  }

  public int run(String[] args) throws Exception {
    String[] toolArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
    if (toolArgs == null || toolArgs.length != 1) {
      usage();
      return 1;
    }

    String outputDir = toolArgs[0];
    getConf().set("TestManyFiles.output_dir", outputDir);
    int num_threads = getConf().getInt("TestManyFiles.num_threads", DEFAULT_NUM_THREADS);
    getConf().setInt("TestManyFiles.num_threads", num_threads);
    int num_writers = getConf().getInt("TestManyFiles.num_writers", DEFAULT_NUM_WRITERS);
    getConf().setInt("TestManyFiles.num_writers", num_writers);
    int file_count = getConf().getInt("TestManyFiles.file_count", DEFAULT_FILE_COUNT);
    getConf().setInt("TestManyFiles.file_count", file_count);
    int file_size = getConf().getInt("TestManyFiles.file_size", DEFAULT_FILE_SIZE);
    getConf().setInt("TestManyFiles.file_size", file_size);
    boolean run_reducer = getConf().getBoolean("TestManyFiles.run_reducer", true);
    getConf().setBoolean("TestManyFiles.run_reducer", run_reducer);

    jobConf = new JobConf(getConf());
    jobConf.setJarByClass(getClass());
    jobConf.setSpeculativeExecution(false);

    jobConf.setMapperClass(Mapper.class);
    jobConf.setInputFormat(InputFormat.class);

    if (run_reducer) {
      jobConf.setNumReduceTasks(1);
      jobConf.setReducerClass(Reducer.class);
    } else {
      jobConf.setNumReduceTasks(0);
    }

    jobConf.setOutputFormat(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(jobConf, new Path(outputDir));

    JobClient.runJob(jobConf);
    return 0;
  }

}
