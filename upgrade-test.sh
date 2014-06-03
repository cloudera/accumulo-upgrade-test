#!/bin/bash
#
# Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
#
# Cloudera, Inc. licenses this file to you under the Apache License,
# Version 2.0 (the "License"). You may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for
# the specific language governing permissions and limitations under the
# License.
 
# Variables you must set based on your cluster
INSTANCE=instance
ROOTPW=secret
ZK_HOSTS=zoo1.example.com,zoo2.example.com,zoo3.example.com
 
# Convenience
ACCUMULO="sudo -i -u accumulo /usr/lib/accumulo/bin/accumulo"
 
# Clean up cruft
for role in tserver logger; do
  pssh -h $ACCUMULO_CONF_DIR/slaves -i 'sudo pkill -f [D]app='$role \; :
done
for role in gc monitor tracer master; do
  pssh -h $ACCUMULO_CONF_DIR/masters -i 'sudo pkill -f [D]app='$role \; :
done
sudo -u hdfs hadoop fs -rm -r /accumulo
  pssh -i -h nodes \
  sudo rm -fv /data/\*/dfs/walogs/\* \; \
  sudo rm /usr/lib/accumulo \; \
  sudo ln -s /usr/lib/accumulo-1.4.4-cdh4.5.0 /usr/lib/accumulo
 
# Init the old
sudo -u hdfs hadoop fs -mkdir /accumulo
sudo -u hdfs hadoop fs -chown accumulo: /accumulo
printf "${INSTANCE}\nY\n${ROOTPW}\n${ROOTPW}\n" | $ACCUMULO init
 
pssh -h nodes -i 'sudo su - accumulo -c /usr/lib/accumulo/bin/start-here.sh'
 
# Do the load
printf "${ROOTPW}\n" | /usr/lib/accumulo/bin/tool.sh accumulo-upgrade-tests-1.4.4-cdh4.5.0.jar \
  com.cloudera.accumulo.upgrade.compatibility.DataCompatibilityLoad \
  -libjars jcommander-1.32.jar -u root -i ${INSTANCE} -p \
  -z $ZK_HOSTS
 
for role in tserver logger; do
  pssh -h $ACCUMULO_CONF_DIR/slaves -i 'sudo pkill -f [D]app='$role \; :
done
for role in gc monitor tracer master; do
  pssh -h $ACCUMULO_CONF_DIR/masters -i 'sudo pkill -f [D]app='$role \; :
done
 
# Gather some information
pssh -i -h $ACCUMULO_CONF_DIR/slaves \
  $ACCUMULO org.apache.accumulo.server.logger.LogReader /data/\*/dfs/walogs/\* \
  | grep DEFINE_TABLET
$ACCUMULO org.apache.accumulo.server.fate.Admin print | grep -qv 'BAD CONFIG'
 
if [[ $? -ne 1 ]]; then
  echo "Fate operations outstanding after 1.4 shutdown; exiting..."
  exit 1
fi
 
# Do the upgrade
pscp -h nodes accumulo-1.6.0-cdh4.6.0-SNAPSHOT-bin.tar.gz $HOME
TARGET=/usr/lib/accumulo-1.6.0-cdh4.6.0-upgrade
pssh -h nodes \
  tar xzf accumulo-1.6.0-cdh4.6.0-SNAPSHOT-bin.tar.gz \; \
  sudo rm /var/log/accumulo/\* \; \
  sudo rm -rf $TARGET \; \
  sudo mv accumulo-1.6.0-cdh4.6.0-SNAPSHOT $TARGET \; \
  sudo chown -R accumulo: $TARGET \; \
  sudo rm /usr/lib/accumulo \; \
  sudo ln -s $TARGET /usr/lib/accumulo \; \
  sudo -i -u accumulo $TARGET/bin/build_native_library.sh
 
# WAL Copy
pssh -i -t 0 -h $ACCUMULO_CONF_DIR/slaves \
  $ACCUMULO org.apache.accumulo.tserver.log.LocalWALRecovery
 
# Start 1.6
pssh -h nodes sudo -i -u accumulo /usr/lib/accumulo/bin/start-here.sh
 
# Wait for things to come online..
$ACCUMULO shell -u root -p cloudera -e 'scan -np -t accumulo.root'
$ACCUMULO shell -u root -p cloudera -e 'scan -np -t accumulo.metadata'
 
# Verify
/usr/lib/accumulo/bin/tool.sh accumulo-upgrade-tests-1.6.0-SNAPSHOT.jar \
  com.cloudera.accumulo.upgrade.compatibility.DataCompatibilityVerify \
  -libjars jcommander-1.32.jar -u root -i $INSTANCE -p -z $ZK_HOSTS
 
# Restart and verify again
pssh -h nodes sudo -i -u accumulo /usr/lib/accumulo/bin/stop-here.sh
pssh -h nodes sudo -i -u accumulo /usr/lib/accumulo/bin/start-here.sh
$ACCUMULO shell -u root -p cloudera -e 'scan -np -t accumulo.root'
$ACCUMULO shell -u root -p cloudera -e 'scan -np -t accumulo.metadata'
/usr/lib/accumulo/bin/tool.sh accumulo-upgrade-tests-1.6.0-SNAPSHOT.jar \
  com.cloudera.accumulo.upgrade.compatibility.DataCompatibilityVerify \
  -libjars jcommander-1.32.jar -u root -i $INSTANCE -p -z $ZK_HOSTS
