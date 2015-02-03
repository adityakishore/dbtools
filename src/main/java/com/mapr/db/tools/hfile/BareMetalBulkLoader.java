package com.mapr.db.tools.hfile;

/**
 * Generates and loads HFiles for YCSB
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author kkhatua
 *
 */
public class BareMetalBulkLoader {
  private static final String FIELD_LENGTH = "fieldLength";
  private static final String FIELD_COUNT = "fieldCount";
  private static final String DEFAULT_FIELD_COUNT = "10";
  private static final String DEFAULT_FIELD_SIZE = "100";
  private static final boolean _dBug = true;
  /* Parameters */
  private JobConf jobConf;
  private FileSystem distribFS;
  private Job mrJob;
  private static long totalRowCount;
  private static String workPath;
  private static String tableName;
  private static String usrDefFieldCount;
  private static String usrDefFieldSize;
  private static Path inputDir;
  private static int maxMapAttempts;
  private static Path outputDir;
  private static String regionSplitsSrc;
  private static boolean useRegionsFile;
  private static boolean isM7Table;

  /**
   * Dummy MapTask
   */
  public static class YCSBReducer extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable,
  ImmutableBytesWritable, KeyValue>
  implements JobConfigurable {
    /*CONSTANTS*/
    private static final char ZERO_CHAR = '0';
    private static final char BLANK_SPACE = ' ';
    final private static String DEFAULT_FAMILY_VALUE = "family";
    final private static String KEY_PREFIX = "user";
    final private static String FIELD_PREFIX = "field";
    private static final String DEFAULT_MAX_KEY_SIZE = "19";
    private static final String DEFAULT_KEY_SIZE = "keysize";

    private static int fieldCount;
    private static int fieldLength;
    private static byte[] familyAsBytes;
    private int maxSize;


    public void configure(JobConf jobConfig) {
      //Initializing Generic Properties

      familyAsBytes = Bytes.toBytes(jobConfig.get(DEFAULT_FAMILY_VALUE,DEFAULT_FAMILY_VALUE));
      fieldCount = Integer.parseInt(jobConfig.get(FIELD_COUNT,DEFAULT_FIELD_COUNT)); 
      fieldLength = Integer.parseInt(jobConfig.get(FIELD_LENGTH,DEFAULT_FIELD_SIZE));

      maxSize = Integer.parseInt(jobConfig.get(DEFAULT_KEY_SIZE,DEFAULT_MAX_KEY_SIZE));
      //Initializing CharArray
      digitChar = new char[maxSize];

    }

    private long rows2Write;
    private char[] digitChar;


    protected void reduce(
        ImmutableBytesWritable startKeyParam, Iterable<ImmutableBytesWritable> values,
        org.apache.hadoop.mapreduce.Reducer<ImmutableBytesWritable, ImmutableBytesWritable,ImmutableBytesWritable, KeyValue>.Context context)
            throws IOException, InterruptedException {


      //Initializing Range
      String startKeyStr = Bytes.toString(startKeyParam.get());
      long startKey = Long.valueOf(startKeyStr.substring(KEY_PREFIX.length()));

      Iterator<ImmutableBytesWritable> k = values.iterator();

      //Reading only 1st of the iterables
      if (k.hasNext())
        rows2Write = Long.valueOf(Bytes.toLong(k.next().get()));

      //Representing Rows written in increments of 1 percent (Total output will be 100 lines!)
      long RowsPerCent = rows2Write/100;

      System.out.println("[INFO] Starting with "+startKey+ "... writing "+rows2Write+" rows");

      //Initializing LexiKeyGen
      initLexiKeyGen(startKey);

      //Building Key
      String rowKey = KEY_PREFIX+startKey;

      //Emit ALL the rows
      for (long currRowsWritten = 0; currRowsWritten < rows2Write; currRowsWritten++) {

        //Mappers Emitting to Reducer (HFileOutputFormat)  
        byte[] rowKeyAsBytes = Bytes.toBytes(rowKey);

        @SuppressWarnings("unused")
        boolean emitStatus = buildKVPairs(context, rowKeyAsBytes);

        //KeepAlive Counters
        context.getCounter(KEEP_ALIVE_COUNTER.USERS_DONE).increment(1);

        //Update Status:
        if ((currRowsWritten-startKey)%RowsPerCent == 0) {
          context.setStatus(rowKey);
          System.out.println("[INFO] Done writing.." + rowKey/*context.getStatus()*/ + " ["+(new Date()).toString()+"]");
          context.progress();
        }

        //Generating Next Key
        rowKey = KEY_PREFIX+incrementLexically(digitChar);
      }

    }

    /**
     * Initializes lexical key for generation
     * @param startKeyValue
     */
    private void initLexiKeyGen(long startKeyValue) {
      String startKey = startKeyValue+"";
      //Building CharArray
      for (int i = 0; i < maxSize; i++) {
        if (i < startKey.length())
          digitChar[i] = startKey.charAt(i);
        else
          digitChar[i] = BLANK_SPACE;
      }
    }

    /**
     * Generates key in Lexical Sequence for a given start point
     * @param digits
     * @return
     */
    private String incrementLexically(char[] digits) {
      int indexToReplace = new String(digits).indexOf(BLANK_SPACE);
      if (indexToReplace > 0) {
        digits[indexToReplace] = ZERO_CHAR;
      } else {
        int indexToIncrement = digits.length - 1;
        int sum; 
        do {
          if (indexToIncrement < 0) break;
          sum = ((int)digits[indexToIncrement] + 1);
          //dBug: System.out.println("idx2Incr:"+indexToIncrement+"\tSum:"+sum);
          if (sum > 57) { //Overflow
            if (indexToIncrement > 0) //We dont want fold-over from 9xxx to 0..
              digits[indexToIncrement] = ZERO_CHAR;
          } else {
            digits[indexToIncrement] = (char) sum;
          }

          indexToIncrement--;
        } while (sum > 57);

      }

      return new String(digits);
    }



    /**
     * Emits (Constructs data for each field for a given rowKey (byte[]) 
     * @param context
     * @param rowKeyAsBytes
     * @return
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private boolean buildKVPairs(org.apache.hadoop.mapreduce.Reducer.Context context, byte[] rowKeyAsBytes) {
      //Computing for all fields
      for (int i = 0; i < fieldCount; i++) {

        //Defining column/field
        String fieldname=FIELD_PREFIX+i;  //Specifying Field Name
        byte[] fieldAsBytes = Bytes.toBytes(fieldname);

        //Building Value
        //byte[] value = new byte[fieldLength];
        //r.nextBytes(value);
        byte[] value = new RandomByteIterator(fieldLength /*fieldlengthgenerator.nextInt()*/).toArray();

        KeyValue kvPair = new KeyValue(rowKeyAsBytes,familyAsBytes,fieldAsBytes,value);

        try {
          context.write(new ImmutableBytesWritable(rowKeyAsBytes), kvPair);
        } catch (Exception e) {
          e.printStackTrace();
          return false;
        }
      }

      return true;
    }

  }


  /**
   * Mapper for generating YCSB data
   * @author kkhatua
   */
  public static class YCSBMapper 
  //extends Mapper<LongWritable,LongWritable,  LongWritable, LongWritable> {
  //extends Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
  extends Mapper<LongWritable, LongWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

    /**
     * Define Input Params... based on which, generate output for HFiles
     */
    @Override
    protected void map(LongWritable startKeyParam, LongWritable keyRangeCount, 
        org.apache.hadoop.mapreduce.Mapper<LongWritable, LongWritable, ImmutableBytesWritable, ImmutableBytesWritable>.Context context)
            throws IOException, InterruptedException {

      //Emit row ranges
      String startKeyID = YCSBReducer.KEY_PREFIX+startKeyParam.get();
      Long rangeSize = keyRangeCount.get();
      System.out.println("Emitting "+startKeyID+" for "+rangeSize+" rows");
      context.write(new ImmutableBytesWritable(Bytes.toBytes(startKeyID)), new ImmutableBytesWritable(Bytes.toBytes(rangeSize)));
    }


    protected void mapOLD(ImmutableBytesWritable startKeyParam, ImmutableBytesWritable keyRangeCount, 
        org.apache.hadoop.mapreduce.Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable>.Context context)
            throws IOException, InterruptedException {
      //Emit row ranges
      //      String startKeyID = startKeyParam.get()
      context.write(startKeyParam, keyRangeCount);
    }
  }

  public static enum KEEP_ALIVE_COUNTER {
    USERS_DONE
  };

  /**
   * @param args
   */
  public static void main(String[] args) {
    System.out.println(
        "================================================================" + "\n" +
            " HFile Generation & Loader for YCSB  [Ver 1.2 - 20140220]" + "\n" +
            "================================================================" + "\n" 
        );
    //Extract Arguments
    extractArguments(args);

    //Read configuration from file ?
    BareMetalBulkLoader hfileGen = new BareMetalBulkLoader();

    //Prepare and run MR Job for HFile Generations
    hfileGen.prepAndRunJob();

    try { Thread.sleep(2000); } catch (Exception e) {/*DoNothing*/}
    System.exit(0); //Forces Rapid Exit
  }

  private static void extractArguments(String[] args) {
    if (args.length < 4) {
      System.err.println("[ERROR] Incorrect arguments!");
      System.err.println("USAGE: <tableName> <regionSplits> <targetDir> <rowCount> [columnCount] [columnSize]");
      System.exit(127);
    }
    tableName =  args[0];   /* "/tables/usertable"; */
    regionSplitsSrc =  args[1];   /* "regions.10.lst"; */

    workPath = args[2];   /* "/dummy"; */
    totalRowCount = Long.valueOf(args[3]); /* 1077; */
    if (args.length > 4)
      usrDefFieldCount = args[4];
    if (args.length > 5)
      usrDefFieldSize = args[5];

    maxMapAttempts = 2; /*DEFAULT 2 Attempts*/

    //Checking if a table is M7
    isM7Table = tableName.contains("/");

    //Check if to use RegionSplits or Table
    File regionSplitsFile = new File(regionSplitsSrc);
    if (regionSplitsFile.exists()) 
      useRegionsFile = true;
    else {
      System.out.println("[WARNING] "+regionSplitsSrc+" does not exist. Will look at table partitions for #reducers");
      useRegionsFile = false;
    }

    //Fail-Safe Condition
    if (useRegionsFile && !isM7Table) {
      System.err.println("[ERROR] User Defined RegionSplits are not allowed for Non-M7 tables! Please rerun the commmand a dummy filename");
      System.err.print("Actual Args: \t");
      for (int i = 0; i < args.length; i++) System.err.print(args[i]+" ");
      System.err.println();

      System.err.print("Suggested Args: \t");
      for (int i = 0; i < args.length; i++) 
        if (i == 1)   System.err.print(args[i]+".null"+" ");
        else      System.err.print(args[i]+" ");
      System.err.println();

      System.exit(127);
    }

  }

  /**
   * Prepares Job & Executes
   */
  private void prepAndRunJob() {
    try {
      //Creating Job Configuration using HBase Template
      jobConf = new JobConf(HBaseConfiguration.create());

      /* Setting up Job Config */
      jobConf.setJar(getClass().getSimpleName()+".jar");

      //YCSB Table Info
      if (usrDefFieldCount != null)
        jobConf.set(FIELD_COUNT, usrDefFieldCount);
      if (usrDefFieldSize != null)
        jobConf.set(FIELD_LENGTH, usrDefFieldSize);
      //Job Name
      jobConf.setJobName("BareMetal BulkLoader"
          + "("+jobConf.get(FIELD_COUNT, DEFAULT_FIELD_COUNT)+"x"+jobConf.get(FIELD_LENGTH, DEFAULT_FIELD_SIZE)+")");

      //Additional Config
      //SetMaxAttempts 
      jobConf.setMaxMapAttempts(maxMapAttempts);

      //Defining Destination of HFiles
      outputDir = new Path(workPath);
      //Temporary Directory
      inputDir = getNewTempDir(outputDir);
      createNewDFSDir(inputDir, true);
      FileInputFormat.setInputPaths(jobConf, inputDir);

      HTable table = new HTable(jobConf, tableName);

      //Prepare Control files
      long startTime = System.currentTimeMillis();
      int numReducers = prepareMapperInputs(totalRowCount,inputDir.makeQualified(distribFS), table);
      long timeToReadyMapInputs = System.currentTimeMillis() - startTime;
      System.out.println("[TIME] Prepare "+numReducers+" Map Inputs: "+(timeToReadyMapInputs/1000) + " sec");

      //inputsReady = false; //dBug
      if (numReducers > 0) {

        //Retrieve a Configured job
        if (_dBug) System.out.println("Configuring Job");
        mrJob = configureNewJob(jobConf);

        //--- Reducer Component isnt explicitly written ---
        if (isM7Table)
          HFileOutputFormat2.configureMapRTablePath(mrJob, tableName);
        else
          HFileOutputFormat2.configureIncrementalLoad(mrJob, table);

        boolean directoryExists = existsDFSDir(outputDir, true);
        FileOutputFormat.setOutputPath(mrJob, outputDir);

        System.out.println("[INFO] PartitionsFile available @ " + TotalOrderPartitioner.getPartitionFile(mrJob.getConfiguration()));

        //Start Generating map-reduce job
        System.out.println("[INFO] Starting Job");
        startTime = System.currentTimeMillis();
        //        JobClient.runJob(jobConf); //OldStyle?
        boolean jobStatus = mrJob.waitForCompletion(true);
        long timeToWriteHFiles = System.currentTimeMillis() - startTime;

        if (!jobStatus) 
          throw new Exception("error with job!");       
        System.out.println("[TIME] Generate HFiles : "+(timeToWriteHFiles/1000) + " sec");

        //Bulkload Into the Table
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(jobConf);
        startTime = System.currentTimeMillis();
        loader.doBulkLoad(outputDir, table);
        long timeToBulkLoadHFiles = System.currentTimeMillis() - startTime;

        try {
          if (!jobStatus) 
            throw new Exception("error with job!");
        } catch (Exception e) { e.printStackTrace();
        }
        System.out.println("[TIME] BulkLoad HFiles : "+(timeToBulkLoadHFiles/1000) + " sec");

      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        //Final Cleanup of temporary input directory
        existsDFSDir(inputDir, true);
      } catch (IOException e) {
        e.printStackTrace();
      }
    } 

    System.out.println("[INFO] Location of HFiles on the cluster (Use this for Bulk-loading):");
    System.out.println("HFilePath="+outputDir);

    return;
  }

  /**
   * Returns a newly created&configured job   
   * @param jobConf
   * @return
   * @throws IOException
   * @throws URISyntaxException
   */
  private Job configureNewJob(JobConf jobConf) throws IOException, URISyntaxException {
    //Preparing Job       
    Job newJob = new Job(jobConf/*,"Generate HFiles for YCSB (table=usertable)"*/);

    //FIXME? Use this for UserJar?
    //newJob.setJarByClass(getClass());

    // Defining MapTask input format
    newJob.setInputFormatClass(SequenceFileInputFormat.class);
    newJob.setMapperClass(YCSBMapper.class);

    //HFile:
    //    newJob.setMapOutputKeyClass(LongWritable.class);
    //    newJob.setMapOutputValueClass(LongWritable.class);
    newJob.setMapOutputKeyClass(ImmutableBytesWritable.class);
    newJob.setMapOutputValueClass(ImmutableBytesWritable.class);
    //    newJob.setOutputKeyClass(NullWritable.class);
    //    newJob.setOutputValueClass(NullWritable.class);

    //Configuring Partitions:
    if (useRegionsFile)
      configureUserDefPartitions(newJob);

    //Defining ReduceTask
    newJob.setReducerClass(YCSBReducer.class);

    //Defining Output Classes
    newJob.setOutputKeyClass(ImmutableBytesWritable.class);
    newJob.setOutputValueClass(KeyValue.class);
    newJob.setOutputFormatClass(HFileOutputFormat.class);

    return newJob;
  }

  /**
   * Setting Partitions based on regions in user defined file
   * @param newJob
   * @return
   * @throws IOException
   * @throws URISyntaxException
   */
  private boolean configureUserDefPartitions(Job newJob) throws IOException, URISyntaxException {
    /* List<ImmutableBytesWritable> startKeys = getRegionStartKeys(regionSplitsSrc); */
    List<Long> startKeys = getRegionStartKeys(regionSplitsSrc); 
    System.out.println("[INFO] startKeys:" + startKeys.size());

    /*--- Adding custom partitions ---*/ 
    String uuid = UUID.randomUUID().toString(); 
    //Setting up Partitioner
    newJob.setPartitionerClass(TotalOrderPartitioner.class);
    //Specifying Partition File (MR Job refers to this!)
    Path partitionFile = new Path(newJob.getWorkingDirectory(),
        "customRegions_" + uuid);

    TotalOrderPartitioner.setPartitionFile(newJob.getConfiguration(), partitionFile);
    System.out.println("[INFO] Set partition files as " + partitionFile.toString());

    //Inferring the partitions to write
    writePartitions(partitionFile, startKeys);
    //Setting #reduce Tasks
    newJob.setNumReduceTasks(startKeys.size()); //Subtracted for teh extra Key (1st region)
    System.out.println("[INFO] Set number of ReduceTasks = "+startKeys.size());

    System.out.println("[INFO] Adding partitions file to DistribCache");
    URI partitionUri =  partitionFile.toUri();
    DistributedCache.addCacheFile(partitionUri, newJob.getConfiguration());

    return true;
  }

  /**
   * Writes partitions to partition file
   * @param conf
   * @param partitionsPath
   * @param startKeys
   * @throws IOException
   */
  private void writePartitions(Path partitionsPath, List<Long> startKeys) throws IOException {
    if (startKeys.isEmpty()) 
      throw new IllegalArgumentException("No regions passed");

    System.out.println("[INFO] Writing Partitions...");

    //Creating a partitions file (sorted)
    TreeSet<ImmutableBytesWritable> sorted = new TreeSet<ImmutableBytesWritable>();
    //Adding Keys
    for (Long startKey : startKeys) {
      if (startKey == 0 )   continue; //Skipping
      sorted.add(new ImmutableBytesWritable(Bytes.toBytes(YCSBReducer.KEY_PREFIX+startKey)));
    }

    // Write the actual file
    SequenceFile.Writer writer = SequenceFile.createWriter
        (distribFS, jobConf, partitionsPath, 
            ImmutableBytesWritable.class, NullWritable.class,
            CompressionType.NONE);    

    try {
      for (ImmutableBytesWritable startKey : sorted) {
        System.out.println("\tWrote partition startKey "+Bytes.toString(startKey.get())+" to ("+partitionsPath.toString()+")");
        writer.append(startKey, NullWritable.get());
      }
    } finally {
      writer.close();
    }
  }

  /**
   * Create a temporary new Dir
   * @return
   */
  private Path getNewTempDir(Path targetDir) {
    String tempDirName = "ycsbTmp_" + System.currentTimeMillis();
    return new Path(targetDir.getParent(), tempDirName);
  }

  private boolean existsDFSDir(Path targetDir, boolean deleteIfExists) throws IOException {
    //Access FS When Config is ready
    if (distribFS == null)
      distribFS = FileSystem.get(jobConf);

    //
    if (distribFS.exists(targetDir)) {
      if (deleteIfExists) {
        System.err.println("[WARN] Directory " + distribFS.makeQualified(targetDir)
            + " already exists.  Removing it...");
        try { distribFS.delete(targetDir, true);  } 
        catch (IOException dirDelEx) {  dirDelEx.printStackTrace(); }
        System.err.println("[WARN] Removed " + distribFS.makeQualified(targetDir));
      } else {
        //Use This to not perform auto-delete
        throw new IOException("[ERROR] Directory " + distribFS.makeQualified(targetDir)
            + " already exists.  Please remove it first!");
      }
      return true;
    }
    return false;
  }

  /**
   * Creates path on DFS
   * @param targetDir
   * @param deleteIfExists
   * @throws IOException
   */
  private void createNewDFSDir(Path targetDir, boolean deleteIfExists) throws IOException {
    //Access FS When Config is ready
    if (distribFS == null)
      distribFS = FileSystem.get(jobConf);

    //Checking for existence
    existsDFSDir(targetDir, deleteIfExists);

    //Creating & verifying 
    if (!distribFS.mkdirs(targetDir)) {
      throw new IOException("Cannot create input directory " + targetDir);
    }
  }

  /**
   * Creates input files for each Mapper generating file 
   * @param mapSrcDirPath Path of directory to write to
   * @param mapSrcFileName  Name of File
   * @param startKey  Starting Key
   * @param rowsToWrite Number of rows for the range
   * @throws IOException
   */
  void createFile(Path mapSrcDirPath, String mapSrcFileName,
      long startKey, long rowsToWrite) throws IOException {
    SequenceFile.Writer mapSrcWriter = null;

    //Preparing param file & its contents
    Path file = new Path(mapSrcDirPath, mapSrcFileName);
    //*
    LongWritable startKeyLW = new LongWritable(startKey);
    LongWritable rowsPerRangeLW = new LongWritable(rowsToWrite);
    /*
    ImmutableBytesWritable startKeyIBW = new ImmutableBytesWritable(Bytes.toBytes(startKey));
    ImmutableBytesWritable rowsPerRangeIBW = new ImmutableBytesWritable(Bytes.toBytes(rowsToWrite));
    //*/
    //Creating Writer
    mapSrcWriter = 
        SequenceFile.createWriter(
            distribFS, jobConf, file,
            LongWritable.class, LongWritable.class,
            /*ImmutableBytesWritable.class, ImmutableBytesWritable.class,*/
            CompressionType.NONE);

    //Writing to file
    //mapSrcWriter.append(startKeyIBW, rowsPerRangeIBW);
    mapSrcWriter.append(startKeyLW, rowsPerRangeLW);

    mapSrcWriter.close();
  }

  /**
   * Generate input for Map phase
   * @param totalRowCount
   * @param numMaps
   * @param mapSrcDir
   * @return
   */
  int prepareMapperInputs(long totalRowCount, Path mapSrcDir, HTable destTable) {
    int numMappers = 0;

    try {
      LinkedList<Long> splitKeyID = null;
      if (useRegionsFile)
        splitKeyID = getRegionStartKeys(regionSplitsSrc);
      else
        splitKeyID = getRegionStartKeys(destTable);


      if (splitKeyID.size() == 0) {
        //Figure out New Set of Partitions?
      }


      long rowsPerRange = totalRowCount / splitKeyID.size();
      long rowsAccountedFor = 0;
      long surplusRows = 0;

      for (Long splitKeyLong : splitKeyID) {
        String mapSrcFileName = "range"+numMappers;

        //Checking and adding surplus rows
        if (numMappers == 0) {
          surplusRows = totalRowCount % splitKeyID.size();
          rowsAccountedFor += surplusRows;
        }

        //Write the file
        createFile(mapSrcDir, mapSrcFileName, 
            splitKeyLong, 
            rowsPerRange + (numMappers == 0 ? surplusRows : 0));

        //Accounting
        rowsAccountedFor += rowsPerRange;

        //Incr File Name
        numMappers++;

        //dBug: 
        System.out.println("[INFO] Prepared Reducer#"+numMappers+" [StartKey="+YCSBReducer.KEY_PREFIX+splitKeyLong+" ("+rowsPerRange+" rows)] -> "+mapSrcDir+"/"+mapSrcFileName );
      }
    } catch (IOException e) {
      e.printStackTrace();
      /*Abort and clean up*/
      System.err.println("[CLEANUP] Deleting all " + numMappers + " files");
      try {
        if (existsDFSDir(inputDir, true))
          System.err.println("[CLEANUP] Done!");
      } catch (IOException cleanupEx) { cleanupEx.printStackTrace();
      }
      return 0;
    }

    System.out.println("[INFO] Wrote input params for "+numMappers+" mappers [ "+mapSrcDir+" ]");
    return numMappers;
  }

  /**
   * Return the start keys of all of the regions in this file, as a list of Long
   * @param startKeysFilePath
   * @return
   * @throws IOException
   */
  //private static List<ImmutableBytesWritable> getRegionStartKeys(String startKeysFilePath) throws IOException {
  private static LinkedList<Long> getRegionStartKeys(String startKeysFilePath) throws IOException {
    BufferedReader splitFileReader = new BufferedReader(new FileReader(startKeysFilePath));
    //ArrayList<ImmutableBytesWritable> regionKeyList = new ArrayList<ImmutableBytesWritable>();
    LinkedList<Long> regionKeyList = new LinkedList<Long>();
    String startKeyRead;
    //Adding Initial Key (since 1st region has no startKey 
    regionKeyList.add(0L);
    while ( (startKeyRead = splitFileReader.readLine()) != null ) {
      //dBug:System.out.println("[INFO] Creating Region with StartKey: " + startKeyRead);
      //Adding to List
      regionKeyList.add(Long.valueOf(startKeyRead.trim().substring(YCSBReducer.KEY_PREFIX.length())));
    }

    //Closing File
    splitFileReader.close();

    //Returning SplitKeys
    return regionKeyList;
  }

  /**
   * Return the start keys of all of the regions in this table, as a list of Long
   * @param table
   * @return
   * @throws IOException
   */
  private static LinkedList<Long> getRegionStartKeys(HTable table)
      throws IOException {
    byte[][] byteKeys = table.getStartKeys();
    LinkedList<Long> splitKeyList = new LinkedList<Long>();
    for (byte[] byteKey : byteKeys) {
      //dBug:System.out.println("[INFO] Creating Region with StartKey: " + Bytes.toString(byteKey).trim());     
      //Adding to List
      String splitKey = Bytes.toString(byteKey).trim();
      if (splitKey.length() == 0) //Accounting for 1st Column
        splitKeyList.add(0L);
      else
        splitKeyList.add(Long.valueOf(Bytes.toString(byteKey).trim().substring(YCSBReducer.KEY_PREFIX.length())));
    }

    return splitKeyList;
  }


}