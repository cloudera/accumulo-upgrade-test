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
 
# Assumptions:
# 0) Cluster has LZO set up in a way Accumulo can use (see CDH-17048)
# 1) ACCUMULO_CONF_DIR is set to point to the current configs
# 2) LOCAL_WALOGS dir already exists
# 3) file with all nodes (def cluster.txt)
# 4) access to tarball of 1.4.x and 1.6.x
# 5) access to upgrade test tools compiled against each
# 6) all gc, tracer, monitor roles also in masters
# 7) ACCUMULO_CONF_DIR/monitor has a single line

# Variables you must set based on your cluster
INSTANCE=${INSTANCE:-instance}
ROOTPW=${ROOTPW:-secret}
ZK_HOSTS=${ZK_HOSTS:-zoo1.example.com,zoo2.example.com,zoo3.example.com}
LOCAL_WALOGS=${LOCAL_WALOGS:-/data/\*/accumulo/walogs\/*}
HOSTS=${HOSTS:-cluster.txt}
UPGRADE_TEST_DIR=${UPGRADE_TEST_DIR:-accumulo-upgrade-test}
TARBALL_14=${TARBALL_14:-accumulo-1.4.4-cdh4.5.0.tar.gz}
UPGRADE_TEST_14_JAR=${UPGRADE_TEST_14_JAR:-${UPGRADE_TEST_DIR}/accumulo-upgrade-tests-1.4.4-cdh4.5.0.jar}
TARBALL_16=${TARBALL_16:-accumulo-1.6.0-cdh4.6.0-SNAPSHOT.tar.gz}
UPGRADE_TEST_16_JAR=${UPGRADE_TEST_16_JAR:-${UPGRADE_TEST_DIR}/accumulo-upgrade-tests-1.6.0.jar}
NUM_MAP_SLOTS=${NUM_MAP_SLOTS:-20}
NUM_REDUCE_SLOTS=${NUM_REDUCE_SLOTS:-20}

# Sanity check
for i in TARBALL_14 TARBALL_16 UPGRADE_TEST_14_JAR UPGRADE_TEST_16_JAR HOSTS
do
  if [[ ! -r ${!i} ]]; then
    echo "Invalid value set for $i"
    exit 1
  fi
done

# Convenience
ACCUMULO="sudo -i -u accumulo /usr/lib/accumulo/bin/accumulo"

echo "Clean up."
# Clean up cruft
for role in tserver logger; do
  pssh -h $ACCUMULO_CONF_DIR/slaves -i 'sudo pkill -f [D]app='$role \; :
done
for role in gc monitor tracer master; do
  pssh -h $ACCUMULO_CONF_DIR/masters -i 'sudo pkill -f [D]app='$role \; :
done
sudo -u hdfs hdfs dfs -rm -r /accumulo /user/accumulo
# TODO kill outstanding MR jobs, in case they are data load/verify runs.
hdfs dfs -rm -r data-compatibility-verify
pssh -i -h ${HOSTS} \
  sudo rm -fv ${LOCAL_WALOGS} \; \
  sudo rm -rf /usr/lib/accumulo*

echo "install ${TARBALL_14}"
# Install the old
pscp --hosts ${HOSTS} ${TARBALL_14} ${HOME}
TARGET=/usr/lib/accumulo-old
pssh -i -h ${HOSTS} \
  sudo rm -rf ${TARGET} \; \
  sudo mkdir -p ${TARGET} \; \
  sudo tar -C ${TARGET} --strip 1 -xzf ${HOME}/${TARBALL_14} \; \
  sudo chown -R accumulo: $TARGET \; \
  sudo -i alternatives --install /usr/lib/accumulo accumulo-lib ${TARGET} 50 \; \
  sudo -i alternatives --set accumulo-lib ${TARGET} \; \
  sudo -i -u accumulo make --quiet -C ${TARGET}/src/server/src/main/c++/ \; \

echo "Init"
# Init the old
sudo -u hdfs hdfs dfs -mkdir /accumulo /user/accumulo
sudo -u hdfs hdfs dfs -chown accumulo: /accumulo /user/accumulo
printf "${INSTANCE}\nY\n${ROOTPW}\n${ROOTPW}\n" | $ACCUMULO init

pssh -h ${HOSTS} -i 'sudo su - accumulo -c /usr/lib/accumulo/bin/start-here.sh'

echo "Load Data"
# Do the load
printf "${ROOTPW}\n" | /usr/lib/accumulo/bin/tool.sh ${UPGRADE_TEST_14_JAR} \
  com.cloudera.accumulo.upgrade.compatibility.DataCompatibilityLoad \
  -libjars ${UPGRADE_TEST_DIR}/jcommander-1.32.jar -u root -i ${INSTANCE} -p \
  -z $ZK_HOSTS --num-rows ${NUM_MAP_SLOTS}

if [[ $? -ne 0 ]]; then
  echo "Data Load failed. exiting"
  exit 1
fi

echo "Shutdown."
for role in tserver logger; do
  pssh -h $ACCUMULO_CONF_DIR/slaves -i 'sudo pkill -f [D]app='$role \; :
done
for role in gc monitor tracer master; do
  pssh -h $ACCUMULO_CONF_DIR/masters -i 'sudo pkill -f [D]app='$role \; :
done

echo "Review internals."
# Gather some information
echo "You want this to list all of the tables, but especially the metadata table. (will timeout at 5 minutes, might be a problem with lots of walogs per host.)"
pssh -i -t 300 -h $ACCUMULO_CONF_DIR/slaves \
  $ACCUMULO org.apache.accumulo.server.logger.LogReader ${LOCAL_WALOGS} \
  | grep DEFINE_TABLET
$ACCUMULO org.apache.accumulo.server.fate.Admin print | grep -qv 'BAD CONFIG'

if [[ $? -ne 1 ]]; then
  echo "Fate operations outstanding after 1.4 shutdown; exiting..."
  exit 1
fi

echo "Upgrade to ${TARBALL_16}"
# Do the upgrade
echo "INSTALL"
pscp -h ${HOSTS} ${TARBALL_16} $HOME
TARGET=/usr/lib/accumulo-upgrade
pssh -h ${HOSTS} \
  sudo rm /var/log/accumulo/\* \; \
  sudo rm -rf $TARGET \; \
  sudo mkdir -p ${TARGET} \; \
  sudo tar -C ${TARGET} --strip 1 -xzf ${HOME}/${TARBALL_16} \; \
  sudo chown -R accumulo: $TARGET \; \
  sudo -i alternatives --install /usr/lib/accumulo accumulo-lib ${TARGET} 100 \; \
  sudo -i alternatives --set accumulo-lib ${TARGET} \; \
  sudo -i -u accumulo $TARGET/bin/build_native_library.sh

echo "Recover WALs"
# WAL Copy
pssh --hosts ${ACCUMULO_CONF_DIR}/slaves --inline \
  'sudo -u accumulo echo "number WALs: `ls -1 ${LOCAL_WALOGS} | wc -l`"' \; \
  'sudo -u accumulo echo "WAL size: `du -h -c ${LOCAL_WALOGS} | grep total`"'

pssh -i -t 0 -h $ACCUMULO_CONF_DIR/slaves \
  $ACCUMULO org.apache.accumulo.tserver.log.LocalWALRecovery

echo "Start"
# Start 1.6
pssh -h ${HOSTS} -i 'sudo su - accumulo -c /usr/lib/accumulo/bin/start-here.sh'

echo "Wait for upgrade to finish."
# Wait for things to come online..
$ACCUMULO shell -u root -p ${ROOTPW} -e 'scan -np -t accumulo.root'
$ACCUMULO shell -u root -p ${ROOTPW} -e 'scan -np -t accumulo.metadata'

RESULT=""
while [[ "$RESULT" != "<unassignedTablets>0</unassignedTablets>" ]]; do
  sleep 5
  RESULT=$(curl `cat ${ACCUMULO_CONF_DIR}/monitor`:50095/xml 2>/dev/null \
    | grep unassignedTablets)
  echo "$RESULT"
done

echo "Verify data."
# Verify
printf "${ROOTPW}\n" | /usr/lib/accumulo/bin/tool.sh ${UPGRADE_TEST_16_JAR} \
  com.cloudera.accumulo.upgrade.compatibility.DataCompatibilityVerify \
  -libjars ${UPGRADE_TEST_DIR}/jcommander-1.32.jar -u root -i $INSTANCE -p \
  -z $ZK_HOSTS --num-rows ${NUM_MAP_SLOTS} --num-reduce-slots ${NUM_REDUCE_SLOTS}
if [[ $? -ne 0 ]]; then
  echo "Data verification failed. exiting"
  exit 1
fi
hdfs dfs -rm -r data-compatibility-verify

echo "Compact"
# Ask for a compaction
$ACCUMULO shell -u root -p ${ROOTPW} -e 'compact -p data_.*'
$ACCUMULO shell -u root -p ${ROOTPW} -e 'compact -p accumulo..*'

# wait
RESULT=""
while [[ $RESULT != " 0 transactions" ]]; do
  sleep 5
  RESULT=$($ACCUMULO shell -u root -p ${ROOTPW} -e 'fate print')
  echo "$RESULT"
done

echo "restart."
# Restart and verify again
for role in tserver; do
  pssh -h $ACCUMULO_CONF_DIR/slaves -i 'sudo pkill -f [D]app='$role \; :
done
for role in gc monitor tracer master; do
  pssh -h $ACCUMULO_CONF_DIR/masters -i 'sudo pkill -f [D]app='$role \; :
done
pssh -h ${HOSTS} -i 'sudo su - accumulo -c /usr/lib/accumulo/bin/start-here.sh'

# Wait for second online
$ACCUMULO shell -u root -p ${ROOTPW} -e 'scan -np -t accumulo.root'
$ACCUMULO shell -u root -p ${ROOTPW} -e 'scan -np -t accumulo.metadata'
RESULT=""
while [[ "$RESULT" != "<unassignedTablets>0</unassignedTablets>" ]]; do
  sleep 5
  RESULT=$(curl `cat ${ACCUMULO_CONF_DIR}/monitor`:50095/xml 2>/dev/null \
    | grep unassignedTablets)
  echo "$RESULT"
done

echo "verify."
printf "${ROOTPW}\n" | /usr/lib/accumulo/bin/tool.sh ${UPGRADE_TEST_16_JAR} \
  com.cloudera.accumulo.upgrade.compatibility.DataCompatibilityVerify \
  -libjars ${UPGRADE_TEST_DIR}/jcommander-1.32.jar -u root -i $INSTANCE -p \
  -z $ZK_HOSTS --num-rows ${NUM_MAP_SLOTS} --num-reduce-slots ${NUM_REDUCE_SLOTS}

if [[ $? -ne 0 ]]; then
  echo "Data verification failed. exiting"
  exit 1
fi
