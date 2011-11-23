package org.apache.hadoop.hdfs.server.datanode.metrics;

import org.apache.hadoop.hdfs.protocol.BlockMetricsAsLongs;
import org.apache.hadoop.hdfs.protocol.NodeMetricsAsLongs;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;

import static java.lang.Math.abs;
import static java.lang.Math.nextAfter;


/**
 * Created by IntelliJ IDEA.
 * User: bharath
 * Date: 11/6/11
 * Time: 9:21 AM
 *
 * Creates an overall Metrics report by combining details from the
 * DataNode metrics and a DatablockMetrics report.
 */
public class MetricsReport {
  public long[] blockMetricsReport;
  public long[] nodeMetricsReport;
  public double readLoad;
  final static long decimalAccuracy = 1000000;
  public double writeLoad;

  public MetricsReport(NodeMetricsAsLongs nMetrics, BlockMetricsAsLongs bMetrics) {
    blockMetricsReport = bMetrics.getBlockMetricsListAsLongs();
    nodeMetricsReport = nMetrics.getNodeMetricsAsLongs();
  }

  public MetricsReport(long[] metricsReport) {
    blockMetricsReport = new long[metricsReport.length- NodeMetricsAsLongs.LONGS];
    nodeMetricsReport = new long[NodeMetricsAsLongs.LONGS];
    for(int i = 0; i < NodeMetricsAsLongs.LONGS; ++i) {
      blockMetricsReport[i] = metricsReport[i];
    }

    for(int i = NodeMetricsAsLongs.LONGS; i < metricsReport.length; ++i) {
      blockMetricsReport[i-NodeMetricsAsLongs.LONGS] = metricsReport[i];
    }
  }

  /**
   * This returns a list of long representing the metrics report.
   * This is essentially a concatenation of the block and node metrics reports.
   * @return
   */
  public long[] getReportAsLongs() {
    int len = nodeMetricsReport.length + blockMetricsReport.length;

    long[] report = new long[len];

    for (int i = 0; i < nodeMetricsReport.length; ++i) {
      report[i] = nodeMetricsReport[i];
    }

    for(int i = nodeMetricsReport.length; i < len; ++i) {
      report[i - nodeMetricsReport.length] = blockMetricsReport[i];
    }

    return report;
  }
}