Title: Apache Accumulo Upgrade Testing
Notice:    Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.

           Cloudera, Inc. licenses this file to you under the Apache License,
           Version 2.0 (the "License"). You may not use this file except in
           compliance with the License. You may obtain a copy of the License at

               http://www.apache.org/licenses/LICENSE-2.0

           This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
           CONDITIONS OF ANY KIND, either express or implied. See the License for
           the specific language governing permissions and limitations under the
           License.

******************************************************************************
accumulo-upgrade
================

testing around upgrade compat

## Building

The master branch works with upstream 1.6.0-SNAPSHOT. To build against another
version of Accumulo, checkout the appropriate tag first. Then:

```
mvn clean package
```

Use `mvn dependency:copy-dependencies` to gather the dependencies needed to run
the tool.

## Configuring for LZO

Before loading in data, the test cluster must be configured for LZO compression.
Under CM/CDH, there is a "gplextras" parcel that must be installed.
[CM4 instructions][1] are available. This will create a new parcel directory
called "HADOOP_LZO".

Once the parcel is installed, follow the instructions in the
[hadoop-gpl-compression FAQ][2] for modifying core-site.xml to register the
LZO codecs. The properties for MapReduce are not necessary.

For Accumulo 1.4.x, the LZO JARs and native libraries must be symbolically
linked to areas where Accumulo can see them. The JARs can be symlinked into
the CDH parcel directory under lib/hadoop-0.20-mapreduce/lib, although any
directory in general.classpath will work. The native libraries should be
symlinked to the lib/hadoop/lib/native directory; when in doubt, check what
java.library.path is set to for a tablet server process.

## Running a Load

One way to run the data load MapReduce job is to use Accumulo's `tool.sh`
utility, which includes the Accumulo installation's libraries in the classpath.
The only additional library needed to run is JCommander.

Copy both JARs over to a machine with your Accumulo installation. Then:

```
/usr/lib/accumulo/bin/tool.sh accumulo-upgrade-tests-version.jar \
  com.cloudera.accumulo.upgrade.compatibility.DataCompatibilityLoad \
  -libjars jcommander-version.jar -u root -z zkhost1,zkhost2,... \
  -i instancename -p
```

Fill in the appropriate "version" value for the JARs, and use the correct
ZooKeeper hosts and instance name for your cluster. You will be prompted for
the connection password before the job begins.

Run the load tool with `-h` to see a summary of the available options.

## Verifying

To verify loaded data, run the verification MapReduce job in a similar
fashion.

```
/usr/lib/accumulo/bin/tool.sh accumulo-upgrade-tests-version.jar \
  com.cloudera.accumulo.upgrade.compatibility.DataCompatibilityVerify \
  -libjars jcommander-version.jar -u root -z zkhost1,zkhost2,... \
  -i instancename -p
```

Again, run with `-h` to see an option summary.
[1]: http://www.cloudera.com/content/cloudera-content/cloudera-docs/CM4Ent/latest/Cloudera-Manager-Installation-Guide/cmig_install_LZO_Compression.html
[2]: https://code.google.com/a/apache-extras.org/p/hadoop-gpl-compression/wiki/FAQ
