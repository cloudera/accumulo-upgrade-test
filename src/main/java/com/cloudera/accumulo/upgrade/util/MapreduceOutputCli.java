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

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.mapreduce.Job;

public class MapreduceOutputCli {

  public MapreduceOutputCli(ConnectionCli connection) {
    this.connection = connection;
  }

  protected final ConnectionCli connection;

  public void useAccumuloOutputFormat(Job job) throws AccumuloSecurityException {
    useAccumuloOutputFormat(job, null);
  }

  public void useAccumuloOutputFormat(Job job, String table) throws AccumuloSecurityException {
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    AccumuloOutputFormat.setZooKeeperInstance(job, connection.instance, connection.zookeepers);

    PasswordToken token = new PasswordToken(connection.password);

    AccumuloOutputFormat.setConnectorInfo(job, connection.principal, token);
    AccumuloOutputFormat.setCreateTables(job, false);
    AccumuloOutputFormat.setDefaultTableName(job, table);
  }


}
