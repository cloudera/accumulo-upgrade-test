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
package com.cloudera.accumulo.upgrade.util;

import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class MapreduceInputCli {

  private static final Logger log = Logger.getLogger(MapreduceInputCli.class);

  public MapreduceInputCli(ConnectionCli connection) {
    this.connection = connection;
  }

  protected static ConnectionCli connection;

  @Parameter(names = {"--offline"},  required=false, description = "When given, we'll clone the tables we're going to scan, then take the clones offline and read HDFS files directly.")
  public boolean offlineMode = false;

  @Parameter(names = {"--max-maps"}, required=false, description = "Set a maximum number of map slots to use. Default: no maximum.")
  public int maxMaps = -1;

  /**
   * Relies on a command line arg to specify offline mode, so you should always call close if using this version.
   */
  public void useAccumuloInputFormat(Job job, String table) throws IOException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    useAccumuloInputFormat(job, table, offlineMode);
  }

  List<String> clones = new ArrayList<String>();

  /**
   * Iff you use offline mode, you have to call close() when you're done or manually delete the cloned table yourself.
   */
  public void useAccumuloInputFormat(Job job, String table, boolean offline) throws IOException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    job.setInputFormatClass(AccumuloInputFormat.class);
    /* XXX Need to use a method that exists in 1.4 adn 1.5  :( */
    Configuration configuration = job.getConfiguration();
    AccumuloInputFormat.setZooKeeperInstance(configuration, connection.instance, connection.zookeepers);
    final TableOperations ops = connection.getConnector().tableOperations();

    String scan = table;
    if (offline) {
      Random random = new Random();
      scan = table + "_" + String.format("%016x", Math.abs(random.nextLong()));
      ops.clone(table, scan, true, Collections.<String,String>emptyMap(), Collections.<String>emptySet());
      try {
        ops.offline(scan);
      } finally {
        clones.add(scan);
      }
      AccumuloInputFormat.setScanOffline(configuration, true);
    }

    AccumuloInputFormat.setInputInfo(configuration, connection.principal, connection.password.getBytes(), scan, connection.auths);

    if (0 < maxMaps) {
      // set up ranges
      try {
        Set<Range> ranges = ops.splitRangeByTablets(table, new Range(), maxMaps);
        AccumuloInputFormat.setRanges(configuration, ranges);
        AccumuloInputFormat.disableAutoAdjustRanges(configuration);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Clean up any cloned offline tables from offline mode.
   */
  public void close() throws AccumuloException, AccumuloSecurityException {
    final TableOperations ops = connection.getConnector().tableOperations();
    for (String table : clones) {
      try {
        ops.delete(table);
      } catch (Exception exception) {
        log.error("Couldn't delete table '" + table + "'", exception);
      }
    }
  }

}
