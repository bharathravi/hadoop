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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;

import java.util.Iterator;

/**
 * This class provides an interface for accessing node metrics as long values.
 * TODO(bharath): No clue why we need longs, but this is to maintain uniformity
 * since everything else is sent as longs.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NodeMetricsAsLongs  {
  /**
   * A node metric has 5 longs
   *   total reads, total writes, current read load and
   *   current write load and a threshold value.
   */
  public static final int LONGS = 5;

  private long[] nodeMetricsAsLongs = new long[LONGS];
  private static final double PRECISION = 100000;
  public double readLoad;
  public double writeLoad;
  public long totalReads;
  public long totalWrites;
  public double threshold;

  /**
   * Create a node metrics report.
   *
   * @param metrics - A DataNodeMetrics object
   */
  public NodeMetricsAsLongs(DataNodeMetrics metrics) {
    nodeMetricsAsLongs = new long[LONGS];

    // set the readcount
    totalReads = metrics.blocksRead.value();
    totalWrites = metrics.blocksWritten.value();
    readLoad = metrics.window.getReadsPerSecond();
    writeLoad = metrics.window.getWritesPerSecond();
    threshold = metrics.window.readLoadThreshold;
  }

  public NodeMetricsAsLongs(long[] nodeMetricsReport) {
    nodeMetricsAsLongs = nodeMetricsReport;

    // Convert from double to long.
    threshold = (double) nodeMetricsReport[0]/PRECISION;
    readLoad = (double) nodeMetricsReport[1]/PRECISION;
    writeLoad = (double) nodeMetricsReport[2]/PRECISION;
    totalReads = nodeMetricsReport[3];
    totalWrites = nodeMetricsReport[4];
  }

  public long[] getNodeMetricsAsLongs() {
    // Convert back from long to double.
    nodeMetricsAsLongs[0] = (long) (threshold * PRECISION);
    nodeMetricsAsLongs[1] = (long) (readLoad * PRECISION);
    nodeMetricsAsLongs[2] = (long) (writeLoad * PRECISION);
    nodeMetricsAsLongs[3] = totalReads;
    nodeMetricsAsLongs[4] = totalWrites;
    return nodeMetricsAsLongs;
  }
}
