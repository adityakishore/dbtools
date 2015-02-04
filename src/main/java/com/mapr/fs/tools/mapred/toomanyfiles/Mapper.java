package com.mapr.fs.tools.mapred.toomanyfiles;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.mapr.db.tools.hfile.Utils;
import com.mapr.fs.tools.mapred.TestManyFiles;

public class Mapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<Text, Text, LongWritable, Text> {

  private int file_count;
  private int file_size;
  private String outputDir;
  private String mapId;
  private Path mapParentDir;
  private FileSystem fs;
  private int num_threads;
  private boolean run_reducer;

  @Override
  public void configure(JobConf job) {
    try {
      fs = FileSystem.get(job);
      num_threads = job.getInt("TestManyFiles.num_threads", TestManyFiles.DEFAULT_NUM_THREADS);
      file_count = job.getInt("TestManyFiles.file_count", TestManyFiles.DEFAULT_FILE_COUNT);
      file_size = job.getInt("TestManyFiles.file_size", TestManyFiles.DEFAULT_FILE_SIZE);
      run_reducer = job.getBoolean("TestManyFiles.run_reducer", true);
      outputDir = job.get("TestManyFiles.output_dir");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void map(Text key, Text value,
                  OutputCollector<LongWritable, Text> output,
                  Reporter reporter) throws IOException {
    mapId = String.format("%04d", Integer.valueOf(key.toString()));
    mapParentDir = new Path(outputDir, mapId);

    try {
      ExecutorService pool = new ThreadPoolExecutor(
          num_threads, // core thread pool size,
          num_threads, // maximum thread pool size
          1, // time to wait before reducing threads, if more then coreSz
          TimeUnit.HOURS,
          new LinkedBlockingQueue<Runnable>(),
          new ThreadPoolExecutor.CallerRunsPolicy());

      System.out.println("         File path\tStart(ms)\tCreate\tTotal");
      for (int fileId = 0; fileId < file_count; fileId++) {
        pool.submit(new CreateFileTask(reporter, output, fileId));
      }
      pool.shutdown();
      pool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  class CreateFileTask implements Runnable {
    private Path path;
    private Reporter reporter;
    private OutputCollector<LongWritable, Text> output;

    public CreateFileTask(Reporter reporter,
                          OutputCollector<LongWritable, Text> output,
                          int fileId) {
      this.reporter = reporter;
      this.output = output;
      this.path = new Path(mapParentDir, String.format("%05d", fileId));
    }

    @Override
    public void run() {
      long start = System.currentTimeMillis();
      try (FSDataOutputStream out = fs.create(path, true)) {
        long created = System.currentTimeMillis();
        final int num_ints = file_size/4;
        for (int i = 0; i < num_ints ; i++) {
          out.writeInt(Utils.FNVhash32(i));
        }
        out.close();
        long end = System.currentTimeMillis();
        output.collect(
            new LongWritable(run_reducer ? start/1000 : start),
            new Text(String.format("%d\t%d", (created-start), (end-start))));

        System.out.println(String.format("%s\t%d\t%d\t%d", path, start, (created-start), (end-start)));
        reporter.progress();
        reporter.incrCounter("TestManyFiles", "FileCount", 1);
        reporter.incrCounter("TestManyFiles", "DataSize", file_size);
      } catch (IOException e) {
        System.err.println(e.getMessage());
      }
    }

  }

}