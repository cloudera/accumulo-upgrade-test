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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.accumulo.core.conf.Property.*;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

import com.cloudera.accumulo.upgrade.util.ConnectionCli;
import com.cloudera.accumulo.upgrade.util.MapreduceOutputCli;
import com.cloudera.accumulo.upgrade.util.Cli;
import com.cloudera.accumulo.upgrade.util.VisibilityCli;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
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
import org.apache.hadoop.mapreduce.Counter;

import org.apache.log4j.Logger;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

/**
 * MapReduce job to load tables in a variety of Accumulo serializations.
 *
 * If you enable LZO, you must have the codec for Hadoop properly installed.
 *
 * Creates several tables, flexing Accumulo's serialization options:
 *
 * <ul>
 *   <li>Bloom filter on, uncompressed, block cache on
 *   <li>Bloom filter on, snappy compressed, block cache off
 *   <li>Bloom filter off, snappy compressed, block cache on
 *   <li>Bloom filter off, gz compressed, block cache off
 *   <li><em>optionally</em> Bloom filter off, lzo compressed, block cache off
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
  protected static final String[] FAMILIES = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
                                               "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" };

  /**
   * Mapper that outputs directly to all of the configured tables.
   */
  public static class DataLoadMapper extends Mapper<Text, LongWritable, Text, Mutation> {

    private final Text row = new Text();
    private ColumnVisibility visibility;
    private final Text qualifier = new Text();
    private final MessageDigest digest;
    private final Value value = new Value();
    private final Text out = new Text();
    private Collection<String> tables;

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

    private Counter rowCells;

    @Override
    public void map(Text family, LongWritable numCells, Context context) throws IOException, InterruptedException {
      final Counter familyCells = context.getCounter(DataLoadMapper.class.getName(), "family_" + family.toString());
      final Mutation mutation = new Mutation(row);
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
        familyCells.increment(1l);
        rowCells.increment(1l);
      }
      context.progress();
      if (tables.isEmpty()) {
        /* Use the default table */
        context.write(null, mutation);
      } else {
        for(String table : tables) {
          out.set(table);
          context.write(out, mutation);
        }
      }
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      row.set(new StringBuilder(context.getTaskAttemptID().getTaskID().toString()).reverse().toString());
      visibility = new ColumnVisibility(context.getConfiguration().get(VISIBILITY, ""));
      rowCells = context.getCounter(DataLoadMapper.class.getName(), "row_" + row);
      tables = context.getConfiguration().getStringCollection(OUTPUT_TABLE_NAMES);
    }

  }

  /**
   * given a configurable number of rows, generates records for each one of the form:
   *   Key: one letter A through Z, Value: user specified number of qualifiers
   */
  public static class DataLoadInputFormat extends InputFormat<Text, LongWritable> {
    protected static final String NUM_QUALIFIERS = DataLoadInputFormat.class.getName() + ".num_qualifiers";
    protected static final String NUM_ROWS = DataLoadInputFormat.class.getName() + ".num_rows";
    protected static final String ACTIVE_TRACKERS = DataLoadInputFormat.class.getName() + ".active_trackers";

    @Override
    public RecordReader<Text, LongWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      return new DataLoadRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      final long numCells = getNumQualifiersPerFamily(context.getConfiguration());
      final int numRows = getNumRows(context.getConfiguration());
      final List<String> trackers = getActiveTrackers(context.getConfiguration());;
      final List<InputSplit> splits = new ArrayList<InputSplit>(numRows);
      if (trackers.size() == 0) {
        log.warn("Couldn't get set of active trackers on the cluster, map tasks won't have locality information");
        for (int i = 0; i < numRows; i++) {
          splits.add(new MapperSlotInputSplit(numCells, null));
        }
      } else {
        for (int i = 0; i < numRows;) {
          for (int j = 0; j < trackers.size() && i < numRows; j++, i++) {
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
      return new ArrayList<String>(conf.getStringCollection(ACTIVE_TRACKERS));
    }

    public static void setNumQualifiersPerFamily(Job job, long num) {
      job.getConfiguration().setLong(NUM_QUALIFIERS, num);
    }

    public static long getNumQualifiersPerFamily(Configuration conf) {
      return conf.getLong(NUM_QUALIFIERS, DataCompatibilityTestCli.DEFAULT_NUM_QUALIFIERS);
    }

    public static void setNumRows(Job job, int num) {
      job.getConfiguration().setLong(NUM_ROWS, num);
    }

    public static int getNumRows(Configuration conf) {
      return conf.getInt(NUM_ROWS, DataCompatibilityTestCli.DEFAULT_NUM_ROWS);
    }

    public static class MapperSlotInputSplit extends InputSplit implements WritableComparable<MapperSlotInputSplit> {
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
      @Override
      public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, numCells);
      }
      @Override
      public void readFields(DataInput in) throws IOException {
        numCells = WritableUtils.readVLong(in);
      }
      @Override
      public int compareTo(MapperSlotInputSplit other) {
        return (int) (numCells - other.numCells);
      }
    }

    protected static class DataLoadRecordReader extends RecordReader<Text, LongWritable> {
      protected boolean valid;
      protected int current;
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

    gz(new ImmutableMap.Builder<String, String>()
        .put(TABLE_BLOOM_ENABLED.getKey(), Boolean.toString(false))
        .put(TABLE_FILE_COMPRESSION_TYPE.getKey(), "gz")
        .put(TABLE_BLOCKCACHE_ENABLED.getKey(), Boolean.toString(false))
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

  /* TODO add an option to drop tables that exist at the beginning of the run. */
  protected static class DataCompatibilityTestCli {

    @Parameter(names = {"--num-rows"}, required = false, description = "number of rows to use on given tables.")
    public int numRows = -1;

    public static final int DEFAULT_NUM_ROWS = 2;
    public static final long DEFAULT_NUM_QUALIFIERS = 300000l;

    @Parameter(names = {"-n", "--num-qualifiers"}, required = false, description = "number of column qualifiers per column family. effectively sets number of cells per cf. defaults to 300k.")
    public long qualifiers = DEFAULT_NUM_QUALIFIERS;

    @Parameter(names = {"-t", "--tables"}, required = false, description = "comma separated list of pre-existing tables to write to, instead of making our own.")
    public String tables = null;

    @Parameter(names = {"--prefix"}, required = false, description = "prefix to use on generated table names. Defaults to 'data_compatibility_test_'.")
    public String prefix = "data_compatibility_test_";

    @Parameter(names = {"--lzo"}, required = false, description = "elect to include an LZO-compressed table. Requires Hadoop LZO support.")
    public boolean withLzo = false;

    public List<String> getTableNames(TableOperations ops) throws ParameterException, AccumuloException, AccumuloSecurityException {
      return getTableNames(false, ops);
    }

    public List<String> getTableNamesAndConfigureThem(TableOperations ops) throws ParameterException, AccumuloException, AccumuloSecurityException {
      return getTableNames(true, ops);
    }

    protected List<String> getTableNames(boolean change, TableOperations ops) throws ParameterException, AccumuloException, AccumuloSecurityException {
      List<String> names;
      if (null != tables) {
        names = Arrays.asList(tables.split(","));
        for (String name : names) {
          if (!ops.exists(name)) {
            String error = "Table specified that does not exist: " + name;
            log.error(error);
            throw new ParameterException(error);
          }
        }
      } else {
        final TableOptions[] options = TableOptions.values();
        names = new ArrayList<String>(options.length);
        for (TableOptions option : options) {
          if (!withLzo && "lzo".equals(option.getProperties().get(TABLE_FILE_COMPRESSION_TYPE.getKey()))) {
            log.debug("Skipping table options '" + option + "' because they use LZO compression. pass --lzo to include it.");
            continue;
          }
          final String name = prefix + option.toString();
          if (!ops.exists(name)) {
            if (change) {
              log.info("Creating table for option set '" + option + "'");
              try {
                ops.create(name);
              } catch (TableExistsException exception) {
                log.debug("Table created concurrently; skipping.",  exception);
              }
            } else {
              String error = "Generated table name doesn't exist: " + name;
              log.error(error);
              throw new ParameterException(error);
            }
          }
          if (change) {
            for (Map.Entry<String, String> property : option.getProperties().entrySet()) {
              ops.setProperty(name, property.getKey(), property.getValue());
            }
          }
          names.add(name);
        }
      }
      return names;
    }
  }


  protected static class LoadCli extends Cli {
    public final ConnectionCli connection = new ConnectionCli();
    public final VisibilityCli visibility = new VisibilityCli();
    public final DataCompatibilityTestCli test = new DataCompatibilityTestCli();
    public final MapreduceOutputCli output = new MapreduceOutputCli(connection);

    protected void parseArgs(String programName, String[] args, Object ... others) {
      super.parseArgs(programName, args, connection, visibility, test, output);
    }
  };

  protected LoadCli options = new LoadCli();

  @Override
  public int run(String[] args) throws Exception {
    final String jobName = this.getClass().getName();
    options.parseArgs(jobName, args);
    final Job job = new Job(getConf(), jobName);

    if (-1 == options.test.numRows) {
      options.test.numRows = job.getConfiguration().getInt("mapred.map.tasks", DataCompatibilityTestCli.DEFAULT_NUM_ROWS);
    }

    job.setJarByClass(this.getClass());
    
    job.setInputFormatClass(DataLoadInputFormat.class);
    DataLoadInputFormat.inferActiveTrackers(job);
    DataLoadInputFormat.setNumRows(job, options.test.numRows);
    DataLoadInputFormat.setNumQualifiersPerFamily(job, options.test.qualifiers);

    job.getConfiguration().set(VISIBILITY, new String(options.visibility.visibility.getExpression(), "UTF-8"));

    final TableOperations ops = options.connection.getConnector().tableOperations();
 
    final List<String> names = options.test.getTableNamesAndConfigureThem(ops);
    for (String name : names) {
      if (options.test.numRows > ops.getSplits(name, options.test.numRows).size()) {
        final SortedSet<Text> splits = new TreeSet<Text>();
        for (int i = 0; i < options.test.numRows; i++) {
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
    
    options.output.useAccumuloOutputFormat(job);
    
    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DataCompatibilityLoad(), args);
    System.exit(res);
  }

}
