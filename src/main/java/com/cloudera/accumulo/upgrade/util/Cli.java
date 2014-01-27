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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class Cli {

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

  @Parameter(names={"-h", "-?", "--help", "-help"}, help=true)
  public boolean help = false;  

}
