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
import java.util.List;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.cloudera.accumulo.upgrade.util.MapreduceInputCli;
import com.cloudera.accumulo.upgrade.util.ConnectionCli;
import com.cloudera.accumulo.upgrade.util.Cli;

import com.google.common.primitives.Longs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.IStringConverter;

import org.apache.log4j.Logger;

/**
 * Verify a set of tables matches expections set by DataCompatibilityLoad.
 * Given a set of tables, verify they contain only entries of the form:
 *
 * key, sha1(key)
 *
 * Checks counts of number of families, number of rows, and aggregate number of cells per row/family
 * against provided expected values.
 *
 * You should give this comparable arguments to DataCompatibilityLoad for connection, table, auth, and #rows/qualifiers.
 *
 * additionally, output histograms of cells per rowid and cells per family to HDFS for reference later.
 *
 * XXX You must have the LZO Codec for Hadoop installed to run this job.
 *
 *
 * @see DataCompatiblityLoad
 */
public class DataCompatibilityVerify extends Configured implements Tool {

  private static final Logger log = Logger.getLogger(DataCompatibilityVerify.class);

  protected static final String BAD_COUNTER = "bad checksum";
  protected static final String ROW_COUNTER_PREFIX = "row_";
  protected static final String FAMILY_COUNTER_PREFIX = "family_";

  /**
   * verify checksums, send cells counts to reducers
   */
  public static class DataVerifyMapper extends Mapper<Key, Value, Text, LongWritable> {

    private static final LongWritable ONE = new LongWritable(1l);
    private final MessageDigest digest;

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

    private Counter badCells;
    private Text row = new Text();
    private Text family = new Text();
    private final Text bad = new Text();

    @Override
    public void map(Key key, Value value, Context context) throws IOException, InterruptedException {
      digest.reset();
      digest.update(key.getRowData().getBackingArray());
      digest.update(key.getColumnFamilyData().getBackingArray());
      digest.update(key.getColumnQualifierData().getBackingArray());
      digest.update(key.getColumnVisibilityData().getBackingArray());
      digest.update(Longs.toByteArray(key.getTimestamp()));
      row = key.getRow(row);
      family = key.getColumnFamily(family);
      if (Arrays.equals(value.get(), digest.digest())) {
        context.getCounter(DataVerifyMapper.class.getName(), ROW_COUNTER_PREFIX + row).increment(1l);
        context.write(row, ONE);
        context.getCounter(DataVerifyMapper.class.getName(), FAMILY_COUNTER_PREFIX + family).increment(1l);
        context.write(family, ONE);
      } else {
        badCells.increment(1l);
        bad.set("bad_checksum_" + row);
        context.write(bad, ONE);
        bad.set("bad_checksum_" + family);
        context.write(bad, ONE);
        if(1000 < badCells.getValue()) {
          log.error("Bad checksum for key '" + key  + "' (will only print ~1000 occurances)");
        }
      }
      context.progress();
    }
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      badCells = context.getCounter(DataVerifyMapper.class.getName(), BAD_COUNTER);
    }
  }

  protected static class DataCompatibilityTestCli extends com.cloudera.accumulo.upgrade.compatibility.DataCompatibilityLoad.DataCompatibilityTestCli {
    @Parameter(names = {"-r", "--num-reduce-slots"}, required = false, description = "total number of Reduce slots to take up across all jobs. will default number of reduce slots in cluster. will make sure there is atleast 1 per table to verify.")
    public int numReduceSlots= -1;

    @Parameter(names = {"--output"}, converter = PathConverter.class, required = false, description = "Directory to place job output directories into. defaults to 'data-compatibility-verify' in the current user's hdfs home.")
    public Path output = new Path("data-compatibility-verify");

    public static class PathConverter implements IStringConverter<Path> {
      @Override
      public Path convert(String value) {
        return new Path(value);
      }
    }

  }

  protected static class VerifyCli extends Cli {
    public final ConnectionCli connection = new ConnectionCli();
    public final DataCompatibilityTestCli test = new DataCompatibilityTestCli();
    public final MapreduceInputCli input = new MapreduceInputCli(connection);

    protected void parseArgs(String programName, String[] args, Object ... others) {
      super.parseArgs(programName, args, connection, test, input);
    }
  };

  protected VerifyCli options = new VerifyCli();

  @Override
  public int run(String[] args) throws Exception {
    final String jobName = this.getClass().getName();
    options.parseArgs(jobName, args);
    try {
      final int totalMapSlots = getConf().getInt("mapred.map.tasks", DataCompatibilityTestCli.DEFAULT_NUM_ROWS);
      if (-1 == options.test.numRows) {
        options.test.numRows = totalMapSlots;
      }
      final TableOperations ops = options.connection.getConnector().tableOperations();
      final List<String> names = options.test.getTableNames(ops);
      int totalReduceSlots = getConf().getInt("mapred.reduce.tasks", 0);
      if (-1 != options.test.numReduceSlots) {
        totalReduceSlots = options.test.numReduceSlots;
      }
      if (0 == totalReduceSlots) {
        totalReduceSlots = names.size();
      }
      final int reducesPerJob = Math.max(1, totalReduceSlots / names.size());
      
      final List<Job> jobs = new ArrayList();
      for (String name : names) {
        final Job job = new Job(getConf(), jobName + " " + name);
        job.setJarByClass(this.getClass());
        options.input.useAccumuloInputFormat(job, name);
        job.setMapperClass(DataVerifyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setReducerClass(LongSumReducer.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(options.test.output, name));
        job.setNumReduceTasks(reducesPerJob);
        job.submit();
        jobs.add(job);
      }
  
      boolean success = true;
      final long numCellsPerRow = options.test.qualifiers * DataCompatibilityLoad.FAMILIES.length;
      final long numCellsPerFamily = options.test.qualifiers * options.test.numRows;
      for(Job job : jobs) {
        success &= job.waitForCompletion(true);
        final CounterGroup group = job.getCounters().getGroup(DataVerifyMapper.class.getName());
        if (null == group) {
          log.error("Job '" + job.getJobName() + "' doesn't have counters for the verification mapper.");
          success = false;
        } else {
          final Counter badCounter = group.findCounter(BAD_COUNTER);
          if (null != badCounter && 0 < badCounter.getValue()) {
            log.error("Job '" + job.getJobName() + "' has " + badCounter.getValue() + " entries with bad checksums.");
            success = false;
          }
          int numRows = 0;
          int numFamilies = 0;
          for (Counter counter : group) {
            if (counter.getName().startsWith(ROW_COUNTER_PREFIX)) {
              numRows++;
              if (numCellsPerRow != counter.getValue()) {
                log.error("Job '" + job.getJobName() + "', counter '" + counter.getName() + "' should have " + numCellsPerRow + " cells, but instead has " + counter.getValue());
                success = false;
              }
            } else if (counter.getName().startsWith(FAMILY_COUNTER_PREFIX)) { 
              numFamilies++;
              if (numCellsPerFamily != counter.getValue()) {
                log.error("Job '" + job.getJobName() + "', counter '" + counter.getName() + "' should have " + numCellsPerFamily + " cells, but instead has " + counter.getValue());
                success = false;
              }
            }
          }
          if (options.test.numRows != numRows) {
            log.error("Job '" + job.getJobName() + "' is supposed to have " + options.test.numRows + " rows, but has " + numRows);
            success = false;
          }
          if (DataCompatibilityLoad.FAMILIES.length != numFamilies) {
            log.error("Job '" + job.getJobName() + "' is supposed to have " + DataCompatibilityLoad.FAMILIES.length + " families, but has " + numFamilies);
            success = false;
          }
        }
      }

      return success ? 0 : 1;
    } finally {
      options.input.close();
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DataCompatibilityLoad(), args);
    System.exit(res);
  }
}
