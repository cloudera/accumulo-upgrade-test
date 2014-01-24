/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.accumulo.upgrade.compatibility;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.accumulo.core.conf.Property.*;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;


import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import org.apache.log4j.Logger;

/**
 * MapReduce job to load tables in a variety of Accumulo serializations.
 *
 * XXX You must have the LZO Codec for Hadoop installed to run this job.
 *
 * Creates several tables, flexing Accumulo's serialization options:
 *
 * <ul>
 *   <li>Bloom filter on, uncompressed, block cache on
 *   <li>Bloom filter on, snappy compressed, block cache off
 *   <li>Bloom filter off, snappy compressed, block cache on
 *   <li>Bloom filter off, lzo compressed, block cache off
 * </ul>
 *
 * Then writes a user specified amount of data into each of said tables.
 *
 * Data is written in the form
 *
 * |  row            |   CF    |  CQ                   |          Vis                      |   timestamp   | Value      |
 * | reverse task id |  [A-Z]  |  text one up counter  |  user specified (default blank)   |   now()       |  sha1(key) |
 *
 * With an optional visibility specified by the user.
 *
 * @see DataCompatiblityVerify
 * @see TaskID
 */
public class DataCompatibilityLoad extends Configured implements Tool {

  private static final Logger log = Logger.getLogger(DataCompatibilityLoad.class);

  protected static final String OUTPUT_TABLE_NAMES = DataCompatibilityLoad.class.getName() + ".output-table-names";
  protected static final String VISIBILITY = DataCompatibilityLoad.class.getName() + ".visibility";

  protected static final ColumnVisibility EMPTY_VISIBILITY = new ColumnVisibility(); 
  protected static final Authorizations EMPTY_AUTHORIZATIONS = new Authorizations();

  /**
   * Mapper that outputs directly to all of the configured tables.
   * Keeps all the cells for one row in memory at a time.
   */
  public static class DataLoadMapper extends Mapper<Text, LongWritable, Text, Mutation> {

    private Mutation mutation;
    private ColumnVisibility visibility;
    private final Text qualifier = new Text();
    private final MessageDigest digest;
    private final Value value = new Value();

    {
      MessageDigest temp = null;
      try {
        temp = MessageDigest.getInstance("SHA-1");
      } catch (NoSuchAlgorithmException exception) {
        throw new RuntimeException("Your JVM doesn't have a SHA1 implementation, which we need to run. :(", exception);
      } finally {
        digest = temp;
      }
    }

    private static final String FAMILY_COUNTER_GROUP = DataLoadMapper.class.getName() + ".family_cells";

    @Override
    public void map(Text family, LongWritable numCells, Context context) throws IOException, InterruptedException {
      final Counter counter = context.getCounter(FAMILY_COUNTER_GROUP, family.toString());
      for (long i = 0; i < numCells.get(); i++) {
        long timestamp = System.currentTimeMillis();
        qualifier.set(Long.toString(i));
        digest.reset();
        digest.update(mutation.getRow());
        digest.update(family.getBytes(), 0, family.getLength());
        digest.update(qualifier.getBytes(), 0, qualifier.getLength());
        digest.update(visibility.getExpression());
        digest.update(Longs.toByteArray(timestamp));
        value.set(digest.digest());
        mutation.put(family, qualifier, visibility, timestamp, value);
        counter.increment(1l);
      }
      context.progress();
    }
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      mutation = new Mutation(new StringBuilder(context.getTaskAttemptID().getTaskID().toString()).reverse());
      visibility = new ColumnVisibility(context.getConfiguration().get(VISIBILITY, ""));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      String[] tables = context.getConfiguration().getStrings(OUTPUT_TABLE_NAMES);
      if (tables == null) {
        /* Use the default table. */
        context.write(null, mutation);
      } else {
        final Text out = new Text();
        for(String table : tables) {
          out.set(table);
          context.write(out, mutation);
        }
      }
    }
  }

  /**
   * given a configurable number of splits, generates records for each one of the form:
   *   Key: one letter A through Z, Value: user specified number of qualifiers
   */
  public static class DataLoadInputFormat extends InputFormat<Text, LongWritable> {
    protected static final String NUM_QUALIFIERS = DataLoadInputFormat.class.getName() + ".num_qualifiers";
    protected static final String NUM_SPLITS = DataLoadInputFormat.class.getName() + ".num_splits";
    protected static final String ACTIVE_TRACKERS = DataLoadInputFormat.class.getName() + ".active_trackers";

    @Override
    public RecordReader<Text, LongWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      return new DataLoadRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      final long numCells = getNumQualifiersPerFamily(context.getConfiguration());
      final int numSplits = getNumSplits(context.getConfiguration());
      final List<String> trackers = getActiveTrackers(context.getConfiguration());;
      final List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
      if (trackers.size() == 0) {
        log.warn("Couldn't get set of active trackers on the cluster, map tasks won't have locality information");
        for (int i = 0; i < numSplits; i++) {
          splits.add(new MapperSlotInputSplit(numCells, null));
        }
      } else {
        for (int i = 0; i < numSplits;) {
          for (int j = 0; j < trackers.size() && i < numSplits; j++, i++) {
            splits.add(new MapperSlotInputSplit(numCells, trackers.get(j)));
          }
        }
      }
      return splits;
    }

    public static void inferActiveTrackers(Job job) throws IOException {
      final Configuration conf = job.getConfiguration();
      conf.setStrings(ACTIVE_TRACKERS, (new JobClient(new JobConf(conf))).getClusterStatus().getActiveTrackerNames().toArray(new String[0]));
    }

    public static List<String> getActiveTrackers(Configuration conf) {
      return Arrays.asList(conf.getStrings(ACTIVE_TRACKERS, new String[0]));
    }

    public static void setNumQualifiersPerFamily(Job job, long num) {
      job.getConfiguration().setLong(NUM_QUALIFIERS, num);
    }

    public static long getNumQualifiersPerFamily(Configuration conf) {
      return conf.getLong(NUM_QUALIFIERS, DEFAULT_NUM_QUALIFIERS);
    }

    public static void setNumSplits(Job job, int num) {
      job.getConfiguration().setLong(NUM_SPLITS, num);
    }

    public static int getNumSplits(Configuration conf) {
      return conf.getInt(NUM_SPLITS, DEFAULT_NUM_SPLITS);
    }

    public static class MapperSlotInputSplit extends InputSplit {
      long numCells;
      String[] locations;
      public MapperSlotInputSplit() {
        this(0, null);
      }
      public MapperSlotInputSplit(long num, String tracker) {
        numCells = num;
        locations = null == tracker ? new String[] {} : new String[] { tracker };
      }
      @Override
      public long getLength() {
        return numCells;
      }
      @Override
      public String[] getLocations() {
        return locations;
      }
    }

    protected static class DataLoadRecordReader extends RecordReader<Text, LongWritable> {
      protected boolean valid;
      protected int current;
      protected static final String[] FAMILIES = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
                                                   "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" };
      protected final Text key = new Text();
      protected final LongWritable value = new LongWritable();

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        current = -1;
        valid = false;
        key.clear();
        value.set(getNumQualifiersPerFamily(context.getConfiguration()));
      }
      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        current++;
        valid = current >= 0 && current < FAMILIES.length;
        if (valid) {
          key.set(FAMILIES[current]);
        }
        return current >= 0 && current < FAMILIES.length;
      }
      @Override
      public Text getCurrentKey() throws IOException, InterruptedException {
        return valid ? key : null;
      }
      @Override
      public LongWritable getCurrentValue() throws IOException, InterruptedException {
        return valid ? value : null;
      }
      @Override
      public float getProgress() throws IOException, InterruptedException {
        return current < 0 ? 0.f : current > FAMILIES.length ? 1.f : (current + 1.0f) / FAMILIES.length;
      }
      @Override
      public void close() throws IOException {
        current = -1;
        valid = false;
      }
    }

  }

  protected enum TableOptions {
    uncompressed_bloom_block(new ImmutableMap.Builder<String, String>()
        .put(TABLE_BLOOM_ENABLED.getKey(), Boolean.toString(true))
        .put(TABLE_FILE_COMPRESSION_TYPE.getKey(), "none")
        .put(TABLE_BLOCKCACHE_ENABLED.getKey(), Boolean.toString(true))
        .build()),

    snappy_bloom(new ImmutableMap.Builder<String, String>()
        .put(TABLE_BLOOM_ENABLED.getKey(), Boolean.toString(true))
        .put(TABLE_FILE_COMPRESSION_TYPE.getKey(), "snappy")
        .put(TABLE_BLOCKCACHE_ENABLED.getKey(), Boolean.toString(false))
        .build()),

    snappy_block(new ImmutableMap.Builder<String, String>()
        .put(TABLE_BLOOM_ENABLED.getKey(), Boolean.toString(false))
        .put(TABLE_FILE_COMPRESSION_TYPE.getKey(), "snappy")
        .put(TABLE_BLOCKCACHE_ENABLED.getKey(), Boolean.toString(true))
        .build()),

    lzo(new ImmutableMap.Builder<String, String>()
        .put(TABLE_BLOOM_ENABLED.getKey(), Boolean.toString(false))
        .put(TABLE_FILE_COMPRESSION_TYPE.getKey(), "lzo")
        .put(TABLE_BLOCKCACHE_ENABLED.getKey(), Boolean.toString(false))
        .build());

    final Map<String, String> properties;
    TableOptions(ImmutableMap<String, String> properties) {
      this.properties = properties;
    }
    public Map<String, String> getProperties() {
      return properties;
    }
  }

  /* TODO most of this is pulled out of 1.5's cli.Help hierarchy, move it into a 1.4 to 1.5 compat layer */

  @Parameter(names = {"-u", "--user"}, description = "Connection user")
  public String principal = System.getProperty("user.name");
  
  @Parameter(names = {"-p", "--password"}, description = "Connection password", password= true)
  public String password = null;

  @Parameter(names = {"-z", "--keepers"}, description = "Comma separated list of zookeeper hosts (host:port,host:port). defaults to 'localhost' on default ZK port.")
  public String zookeepers = "localhost:2181";
  
  @Parameter(names = {"-i", "--instance"}, description = "The name of the accumulo instance")
  public String instance = null;
  
  @Parameter(names = {"-auths", "--auths"}, converter = AuthConverter.class, description = "the authorizations to use when reading or writing")
  public Authorizations auths = EMPTY_AUTHORIZATIONS;

  @Parameter(names = {"-v", "--visibility"}, converter = VisibilityConverter.class, description = "the visibility expression to user when writing cells. defaults to blank.")
  public ColumnVisibility visibility = EMPTY_VISIBILITY;
  
  @Parameter(names={"-h", "-?", "--help", "-help"}, help=true)
  public boolean help = false;  

  @Parameter(names = {"-t", "--tables"}, required = false, description = "comma separated list of pre-existing tables to write to, instead of making our own.")
  public String tables = null;

  @Parameter(names = {"--prefix"}, required = false, description = "prefix to use on generated table names. Defaults to 'data_compatibility_test_'.")
  public String prefix = "data_compatibility_test_";

  @Parameter(names = {"--num-splits"}, required = false, description = "number of splits to use on given tables. we'll generate one mapper for each. defaults to # of MR slots.")
  public int numSplits = -1;

  protected static final int DEFAULT_NUM_SPLITS = 2;
  protected static final long DEFAULT_NUM_QUALIFIERS = 300000l;

  @Parameter(names = {"-n", "--num-qualifiers"}, required = false, description = "number of column qualifiers per column family. effectively sets number of cells per cf. defaults to 300k.")
  public long qualifiers = DEFAULT_NUM_QUALIFIERS;

  /* TODO add an option to drop tables that exist at the beginning of the run. */

  @Override
  public int run(String[] args) throws Exception {
    final String jobName = this.getClass().getName();
    parseArgs(jobName, args);
    final Job job = new Job(getConf(), jobName);

    if (-1 == numSplits) {
      numSplits = job.getConfiguration().getInt("mapred.map.tasks", DEFAULT_NUM_SPLITS);
    }

    job.setJarByClass(this.getClass());
    
    job.setInputFormatClass(DataLoadInputFormat.class);
    DataLoadInputFormat.inferActiveTrackers(job);
    DataLoadInputFormat.setNumSplits(job, numSplits);
    DataLoadInputFormat.setNumQualifiersPerFamily(job, qualifiers);

    job.getConfiguration().set(VISIBILITY, visibility.toString());

    List<String> names;
    final TableOperations ops = new ZooKeeperInstance(instance, zookeepers).getConnector(principal, password).tableOperations();
 
    if (null != tables) {
      names = Arrays.asList(tables.split(","));
    } else {
      final TableOptions[] options = TableOptions.values();
      names = new ArrayList<String>(options.length);
      for (TableOptions option : options) {
        final String name = prefix + option.toString();
        if (!ops.exists(name)) {
          try {
            ops.create(name);
          } catch (TableExistsException exception) {
            log.debug("Table created concurrently.",  exception);
          }
        }
        for (Map.Entry<String, String> property : option.getProperties().entrySet()) {
          ops.setProperty(name, property.getKey(), property.getValue());
        }
        names.add(name);
      }
    }

    for (String name : names) {
      if (numSplits > ops.getSplits(name, numSplits).size()) {
        final SortedSet<Text> splits = new TreeSet<Text>();
        for (int i = 0; i < numSplits; i++) {
          splits.add(new Text(new StringBuilder(Long.toString(i)).reverse().toString()));
        }
        ops.addSplits(name, splits);
      }
    }

    job.getConfiguration().setStrings(OUTPUT_TABLE_NAMES, names.toArray(new String[0]));
      
    
    job.setMapperClass(DataLoadMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Mutation.class);
    
    job.setNumReduceTasks(0);
    
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    AccumuloOutputFormat.setZooKeeperInstance(job, instance, zookeepers);
    AccumuloOutputFormat.setOutputInfo(job, principal, password.getBytes(), false, null);
    
    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DataCompatibilityLoad(), args);
    System.exit(res);
  }

  /* TODO pull all of this into a 1.4 to 1.5 compat layer */

  protected void parseArgs(String programName, String[] args, Object ... others) {
    JCommander commander = new JCommander();
    commander.addObject(this);
    for (Object other : others)
      commander.addObject(other);
    commander.setProgramName(programName);
    try {
      commander.parse(args);
    } catch (ParameterException ex) {
      commander.usage();
      exitWithError(ex.getMessage(), 1);
    }
    if (help) {
      commander.usage();
      exit(0);
    }
  }
  
  protected void exit(int status) {
    System.exit(status);
  }
  
  protected void exitWithError(String message, int status) {
    System.err.println(message);
    exit(status);
  }

  public static class AuthConverter implements IStringConverter<Authorizations> {
    @Override
    public Authorizations convert(String value) {
      return new Authorizations(value.split(","));
    }
  }

  public static class VisibilityConverter implements IStringConverter<ColumnVisibility> {
    @Override
    public ColumnVisibility convert(String value) {
      return new ColumnVisibility(value);
    }
  }
}
