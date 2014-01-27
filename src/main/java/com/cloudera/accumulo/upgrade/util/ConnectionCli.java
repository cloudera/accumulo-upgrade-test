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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.Connector;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class ConnectionCli {

  public static final Authorizations EMPTY_AUTHORIZATIONS = new Authorizations();
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

  public static class AuthConverter implements IStringConverter<Authorizations> {
    @Override
    public Authorizations convert(String value) {
      return new Authorizations(value.split(","));
    }
  }

  public ZooKeeperInstance getZooKeeperInstance() {
    return new ZooKeeperInstance(instance, zookeepers);
  }

  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    return getZooKeeperInstance().getConnector(principal, password);
  }

}
