/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.metrics;

import static org.apache.hadoop.hdfs.server.common.Util.now;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.NodeMetricsAsLongs;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.source.JvmMetrics;

import java.util.LinkedList;

/**
 *
 * This class is for maintaining  the various DataNode statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 *  for example:
 *  <p> {@link #blocksRead}.inc()
 *
 */
@InterfaceAudience.Private
@Metrics(about="DataNode metrics", context="dfs")
public class DataNodeMetrics {

  @Metric MutableCounterLong bytesWritten;
  @Metric MutableCounterLong bytesRead;
  public @Metric MutableCounterLong blocksWritten;
  public @Metric MutableCounterLong blocksRead;
  @Metric MutableCounterLong blocksReplicated;
  @Metric MutableCounterLong blocksRemoved;
  @Metric MutableCounterLong blocksVerified;
  @Metric MutableCounterLong blockVerificationFailures;
  @Metric MutableCounterLong readsFromLocalClient;
  @Metric MutableCounterLong readsFromRemoteClient;
  @Metric MutableCounterLong writesFromLocalClient;
  @Metric MutableCounterLong writesFromRemoteClient;

  @Metric MutableCounterLong volumeFailures;

  @Metric MutableRate readBlockOp;
  @Metric MutableRate writeBlockOp;
  @Metric MutableRate blockChecksumOp;
  @Metric MutableRate copyBlockOp;
  @Metric MutableRate replaceBlockOp;
  @Metric MutableRate heartbeats;
  @Metric MutableRate blockReports;
  public SlidingWindowDatanodeMetrics window;

  public NodeMetricsAsLongs getNodeMetricsReport() {
    return new NodeMetricsAsLongs(this);
  }

  /**
   *  Approximately maintains the number of read requests per second.
   */
  public class SlidingWindowDatanodeMetrics {
    // Over what period do we calculate threshold?
    // Too small = too rapidly changing threshold, too large = not quick enough.
    // windowSize * recalculationFrequency gives the total time frame over which a load
    // is calculated. Window size = 1 means load is calculated over the time-period = recalculation frequency.
    // For now, windowSize = 3.
    public long windowSizeInSeconds = 3;

    LinkedList<Long> prevReadCounts;
    LinkedList<Long> prevWriteCounts;
    LinkedList<Long> prevTimes;

    public long prevReads;
    public long prevWrites;
    public long prevTime;

    // How often to recalculate metrics (in seconds).
    final static int recalculationFrequencyInSeconds = 10;

    // Reads per second as of the last {@code windowSizeInSeconds} seconds.
    public double readsPerSecond;

    // Writes per second as of the last {@code windowSizeInSeconds} seconds.
    public double writesPerSecond;
    public double readLoadThreshold = 0.07;

    public double getReadsPerSecond() {
      return readsPerSecond;
    }

    public double getWritesPerSecond() {
      return writesPerSecond;
    }

    SlidingWindowDatanodeMetrics() {
      readsPerSecond = 0;
      prevReadCounts = new LinkedList<Long>();
      prevWriteCounts = new LinkedList<Long>();

      prevTimes = new LinkedList<Long>();
      prevReadCounts.add((long) 0);
      prevWriteCounts.add((long) 0);
      prevTimes.add(now());
    }

    public void advanceWindow(Log log) {
      long timeNow = now();
      long lastCalculatedTime = prevTimes.getLast();

      // Don't recalculate load too often.
      if (timeNow - lastCalculatedTime > recalculationFrequencyInSeconds *1000) {
        log.info("Updating...");
        long readsNow = 0;
        long writesNow = 0;

        if (blocksRead != null) {
          readsNow = blocksRead.value();
        }

        if (blocksWritten != null) {
          writesNow = blocksWritten.value();
        }

        log.info("prevReads:" + prevReads + " prev time:" + prevTime
                      + " readsnow:" + readsNow + " timenow:" + timeNow + " windowsize:" + prevReadCounts.size());

        if (prevReadCounts.size() >= windowSizeInSeconds) {
          // If the window has been built up, then calculate load for this window
          prevTime = prevTimes.getFirst();
          prevReads = prevReadCounts.getFirst();
          prevWrites = prevWriteCounts.getFirst();

          readsPerSecond = 1000 * (double) (readsNow - prevReads) / (double) (timeNow - prevTime);
          writesPerSecond = 1000 * (double) (writesNow - prevWrites) / (double) (timeNow - prevTime);
          log.info("Updating1: readloads:" + readsPerSecond);

          // Slide the window, by adding new values and removing old ones
          prevReadCounts.remove();
          prevWriteCounts.remove();
          prevTimes.remove();
        } else {
          // If the window has not yet increased to its max size, current load = total reads/total time
          readsPerSecond = 1000 * (double) (readsNow - 0) / (double) (timeNow - prevTime);
          writesPerSecond = 1000 * (double) (writesNow - 0) / (double) (timeNow - prevTime);
          log.info("Updating: readloads:" + readsPerSecond);
        }

        log.info("Updating old values:");
        prevReadCounts.add(readsNow);
        prevWriteCounts.add(writesNow);
        prevTimes.add(timeNow);
      }
    }
  }

  final MetricsRegistry registry = new MetricsRegistry("datanode");
  final String name;

  public DataNodeMetrics(String name, String sessionId) {
    this.name = name;
    this.window = new SlidingWindowDatanodeMetrics();
    registry.tag(SessionId, sessionId);
  }

  public static DataNodeMetrics create(Configuration conf, String dnName) {
    String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
    MetricsSystem ms = DefaultMetricsSystem.instance();
    JvmMetrics.create("DataNode", sessionId, ms);
    String name = "DataNodeActivity-"+ (dnName.isEmpty()
        ? "UndefinedDataNodeName"+ DFSUtil.getRandom().nextInt() : dnName.replace(':', '-'));
    return ms.register(name, null, new DataNodeMetrics(name, sessionId));
  }

  public String name() { return name; }

  public void addHeartbeat(long latency) {
    heartbeats.add(latency);
  }

  public void addBlockReport(long latency) {
    blockReports.add(latency);
  }

  public void incrBlocksReplicated(int delta) {
    blocksReplicated.incr(delta);
  }

  public void incrBlocksWritten() {
    blocksWritten.incr();
  }

  public void incrBlocksRemoved(int delta) {
    blocksRemoved.incr(delta);
  }

  public void incrBytesWritten(int delta) {
    bytesWritten.incr(delta);
  }

  public void incrBlockVerificationFailures() {
    blockVerificationFailures.incr();
  }

  public void incrBlocksVerified() {
    blocksVerified.incr();
  }

  public void addReadBlockOp(long latency) {
    readBlockOp.add(latency);
  }

  public void addWriteBlockOp(long latency) {
    writeBlockOp.add(latency);
  }

  public void addReplaceBlockOp(long latency) {
    replaceBlockOp.add(latency);
  }

  public void addCopyBlockOp(long latency) {
    copyBlockOp.add(latency);
  }

  public void addBlockChecksumOp(long latency) {
    blockChecksumOp.add(latency);
  }

  public void incrBytesRead(int delta) {
    bytesRead.incr(delta);
  }

  public void incrBlocksRead() {
    blocksRead.incr();
  }

  public void shutdown() {
    DefaultMetricsSystem.shutdown();
  }

  public void incrWritesFromClient(boolean local) {
    (local ? writesFromLocalClient : writesFromRemoteClient).incr();
  }

  public void incrReadsFromClient(boolean local) {
    (local ? readsFromLocalClient : readsFromRemoteClient).incr();
  }

  public void incrVolumeFailures() {
    volumeFailures.incr();
  }
}
