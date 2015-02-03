/* Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.fs.tools.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mapr.db.tools.hfile.Utils;

public class TestManyFiles extends Configured implements Tool {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestManyFiles.class);

  private static final int DEFAULT_NUM_WRITERS = 240;

  private static final int DEFAULT_NUM_THREADS = 8;

  private static final int DEFAULT_FILE_COUNT = 2000;

  private static final int DEFAULT_FILE_SIZE = 2*1024*1024;

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

    jobConf = new JobConf(getConf());
    jobConf.setJarByClass(getClass());
    jobConf.setSpeculativeExecution(false);

    jobConf.setMapperClass(MyMapper.class);
    jobConf.setInputFormat(MyInputFormat.class);

    jobConf.setNumReduceTasks(0);
    FileOutputFormat.setOutputPath(jobConf, new Path(outputDir));

    JobClient.runJob(jobConf);
    return 0;
  }

  public static class MyInputFormat implements InputFormat<Text, Text> {

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
        Text number = new Text(String.valueOf(((MyInputSplit) split).number));

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

  public static class CreateFileTask implements Runnable {
    private Path path;
    private int file_size;
    private FileSystem fs;

    public CreateFileTask(FileSystem fs, Path path, int file_size) {
      this.fs = fs;
      this.path = path;
      this.file_size = file_size;
    }

    @Override
    public void run() {
      try (FSDataOutputStream out = fs.create(path, true)) {
        long start = System.currentTimeMillis();
        final int num_ints = file_size/4;
        for (int i = 0; i < num_ints ; i++) {
          out.writeInt(Utils.FNVhash32(i));
        }
        out.flush();
        long end = System.currentTimeMillis();
        System.out.println(String.format("%s\t%d\t%d", path, start, (end-start)));
      } catch (IOException e) {
        System.err.println(e.getMessage());
      }
    }

  }


  public static class MyMapper implements Mapper<Text, Text, ImmutableBytesWritable, Text> {

    private int file_count;
    private int file_size;
    private String outputDir;
    private JobConf conf;
    private int num_threads;

    @Override
    public void configure(JobConf job) {
      conf = job;
      num_threads = job.getInt("TestManyFiles.num_threads", DEFAULT_NUM_THREADS);
      file_count = job.getInt("TestManyFiles.file_count", DEFAULT_FILE_COUNT);
      file_size = job.getInt("TestManyFiles.file_size", DEFAULT_FILE_SIZE);
      outputDir = job.get("TestManyFiles.output_dir");
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void map(Text key, Text value, OutputCollector<ImmutableBytesWritable, Text> output,
        Reporter reporter) throws IOException {
      FileSystem fs = FileSystem.get(conf);
      Path parent = new Path(outputDir, key.toString());
      ExecutorService pool = new ThreadPoolExecutor(
          num_threads, // core thread pool size,
          num_threads, // maximum thread pool size
          1, // time to wait before reducing threads, if more then coreSz
          TimeUnit.HOURS,
          new LinkedBlockingQueue<Runnable>(),
          new ThreadPoolExecutor.CallerRunsPolicy());

      try {
        for (int i = 0; i < file_count; i++) {
          pool.submit(new CreateFileTask(fs, new Path(parent, String.valueOf(i)), file_size));
          reporter.progress();
          reporter.incrCounter("TestManyFiles", "FileCount", 1);
          reporter.incrCounter("TestManyFiles", "DataSize", file_size);
        }
        pool.shutdown();
        pool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }

  }

}
