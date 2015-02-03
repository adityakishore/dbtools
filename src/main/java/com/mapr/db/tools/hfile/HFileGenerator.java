package com.mapr.db.tools.hfile;

import static com.mapr.db.tools.hfile.Util.ZERO_UNDEF;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;


public class HFileGenerator implements Runnable {

  private static final String BE_VERBOSE = "be.verbose";
  private static final String NUM_TSV_FILES = "num.tsv.files";
  private static final String ROWS_PER_FILE = "rows.per.file";
  private static final String MAX_ACTIVE_WRITERS = "max.active.writers";
  private static final byte[] FAMILY = "family".getBytes();
  private static final byte[][] FIELDS;
  private static int maximumActiveThreads;
  private static long rowsPerFile;
  private static long numOfFiles;
  private static long rowCountPerFile;
  private static CountDownLatch latch;
  private static Semaphore semaphore;

  static {
    FIELDS = new byte[100][];
    for (int i = 0; i < FIELDS.length; i++) {
      FIELDS[i] = ("field"+i).getBytes();
    }
  }

  public static void main(String[] args) {
    main0(new String[]{
        "abc.hfile",
        "1000000"
        });
  }
  /**
   * @param args
   */
  public static void main0(String[] args) {
    try {
      String fileName = args[0];
      Long totalRowCount = Long.valueOf(args[1]);
      if (args.length > 2)
        fieldCount = Integer.parseInt(args[2]);

      if (args.length > 3)
        fieldSize = Integer.parseInt(args[3]);

      init(totalRowCount);

      long startAt = 0L;
      long rowCount = HFileGenerator.rowCountPerFile;
      long accountedRows = 0L;

      Configuration conf = HBaseConfiguration.create();
      conf.set(FileOutputFormat.OUTDIR, "file:///C:/temp/__hfiles__");
      HFileGenerator[] writer = new HFileGenerator[(int)HFileGenerator.numOfFiles];

      for (int i = 0; i < HFileGenerator.numOfFiles; i++) {
        //Check if remaining rows is less than default rowCount
        if (totalRowCount < (accountedRows + rowCount) )
          rowCount = totalRowCount - accountedRows;
        writer[i] = new HFileGenerator(conf , fileName, i, startAt, rowCount);
        //writer[i] = new Thread(new TSVFileGenerator(fileName, i, startAt, rowCount));

        //Accounting
        accountedRows+=rowCount;
        //Next Key
        startAt += rowCount;
      }
      System.out.println("[INFO] Done initializing Writers");

      System.out.println("[INFO] Started writing at " + new Date());
      semaphore = new Semaphore(maximumActiveThreads);
      //Submitting Writers
      ExecutorService executor = Executors.newFixedThreadPool(maximumActiveThreads);
      for (int i = 0; i < HFileGenerator.numOfFiles; i++) {
        semaphore.acquire();
        executor.execute(writer[i]);
      }

      while (semaphore.availablePermits() < maximumActiveThreads) {
        Thread.sleep(1000);
      }

      executor.shutdown();

      System.out.println("[INFO] Finished writing at " + new Date());


    } catch (Exception e) {
      printUsage();
      e.printStackTrace();
    }

  }

  private static void printUsage() {
    System.err.println("USAGE: \n\t"
        + "java [OPTIONS] -jar "+HFileGenerator.class.getSimpleName()+".jar <fileName> <totalRowCount> [columnCount] [columnSize] \n"
        + "\t\t-Ddelimiter=<value> \t\t-Delimiter (Default is TAB) \n"
        + "\t\t-D"+MAX_ACTIVE_WRITERS+"=<value> \t\t- # of active threads writing (Default is 10). This is useful when writing to NFS vs LocalDisk \n"
        + "\t\t-D"+NUM_TSV_FILES+"=<value> \t\t- # of files to generate \n"
        + "\t\t-D"+ROWS_PER_FILE+"=<value> \t\t- # of rows per file (Overrides "+NUM_TSV_FILES+" setting)");

  }

  private static void init(Long totalRowCount) {
    maximumActiveThreads = Integer.valueOf(System.getProperty(MAX_ACTIVE_WRITERS, "1"));

    if (fieldCount == ZERO_UNDEF)
      fieldCount = 10;
    if (fieldSize == ZERO_UNDEF)
      fieldSize = 100;

    numOfFiles = Integer.valueOf(System.getProperty(NUM_TSV_FILES, "1"));

    if (System.getProperty(ROWS_PER_FILE) != null) {
      rowsPerFile = Long.valueOf(System.getProperty(ROWS_PER_FILE, String.valueOf(ZERO_UNDEF)));
      numOfFiles = totalRowCount / rowsPerFile;
      //Accounting for left over rows
      if (totalRowCount % rowsPerFile > 0)
        numOfFiles++;
    }

    if (HFileGenerator.rowsPerFile > 0 )
      rowCountPerFile = HFileGenerator.rowsPerFile;
    else
      rowCountPerFile = totalRowCount/HFileGenerator.numOfFiles;

  }

  private File targetFile;
  private int fileSuffix;
  private long startKey;
  private long rows2Write;
  private Random randomizer;
  private Configuration conf_;
  private static int fieldCount;
  private static int fieldSize;

  public HFileGenerator(Configuration conf, String fileName, int sequenceID, long startAt, long rowCount) {
    this.conf_ = conf;
    this.fileSuffix = sequenceID;
    this.targetFile = new File(fileName+"."+fileSuffix);
    this.startKey = startAt;
    this.rows2Write = rowCount;
    this.randomizer = new Random(System.nanoTime());
  }


  @Override
  public void run() {
    long startTime = System.currentTimeMillis();
    try {
      writeDataFile();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    long duration = System.currentTimeMillis() - startTime;
    if (Boolean.valueOf(System.getProperty(BE_VERBOSE, "true")))
      System.out.println("Writer #"+fileSuffix+" wrote "+rows2Write+" rows in "+duration+" msec ["+targetFile.getAbsolutePath()+"]");
    //Punching out!
    if (latch != null)
      latch.countDown();
    if (semaphore != null)
      semaphore.release();
  }

  private void writeDataFile() throws InterruptedException {
    try {
      TaskAttemptContext ctx = new TaskAttemptContextImpl(conf_, new TaskAttemptID(new TaskID(new JobID("_write_hfile_", fileSuffix), TaskType.REDUCE, fileSuffix), fileSuffix));

      HFileOutputFormat2 opf = new HFileOutputFormat2();
      RecordWriter<ImmutableBytesWritable, Cell> writer = opf.getRecordWriter(ctx);

      for (long i = ZERO_UNDEF; i < rows2Write; i++) {
        byte[] rowBytes = Util.key(startKey);
        Cell[] cells = generateData(rowBytes);
        ImmutableBytesWritable row = new ImmutableBytesWritable(rowBytes);
        for (Cell cell : cells) {
          writer.write(row, cell);
        }
        startKey++;
      }

      writer.close(ctx);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private Cell[] generateData(byte[] row) {
    Cell[] cells = new Cell[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      cells[i] = new KeyValue(row, FAMILY, FIELDS[i], Util.randomString(randomizer, fieldSize).getBytes());
    }

    return cells;
  }


}
