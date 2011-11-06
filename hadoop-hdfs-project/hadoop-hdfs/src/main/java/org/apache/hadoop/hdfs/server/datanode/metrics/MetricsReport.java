package org.apache.hadoop.hdfs.server.datanode.metrics;

import org.apache.hadoop.hdfs.protocol.BlockMetricsAsLongs;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;

import static java.lang.Math.abs;


/**
 * Created by IntelliJ IDEA.
 * User: bharath
 * Date: 11/6/11
 * Time: 9:21 AM
 *
 * Creates an overall Metrics report by combining details from the
 * DataNode metrics and a BlockMetrics report.
 */
public class MetricsReport {
  public long[] blockMetricsReport;
  public double readLoad;
  final static long decimalAccuracy = 1000000;
  public double writeLoad;

  public MetricsReport(DataNodeMetrics metrics, BlockMetricsAsLongs bReport) {
    blockMetricsReport = bReport.getBlockMetricsListAsLongs();
    readLoad = metrics.window.getReadsPerSecond();
    writeLoad = metrics.window.getWritesPerSecond();
  }

  public MetricsReport(long[] metricsReport) {
    blockMetricsReport = new long[metricsReport.length-2];
    readLoad = (double)metricsReport[0]/(double)decimalAccuracy;
    writeLoad = (double)metricsReport[1]/(double)decimalAccuracy;

    for(int i = 2; i < metricsReport.length; ++i) {
      blockMetricsReport[i-2] = metricsReport[i];
    }
  }

  /**
   * This returns a list of long representing the metrics report.
   * list[0] = a long of the readLoad * 100000 (a double represented with an
   *   accuracy of 6 decimal places)
   * list[1-n] = DataBlockMetricsReport.long
   * @return
   */
  public long[] getReportAsLongs() {
    long readLoadLong = (long) (readLoad * decimalAccuracy);
    long writeLoadLong = (long) (writeLoad * decimalAccuracy);
    int len = blockMetricsReport.length + 2;

    long[] report = new long[len];
    report[0] = readLoadLong;
    report[1] = writeLoadLong;

    for(int i = 0; i < blockMetricsReport.length; ++i) {
      report[i+2] = blockMetricsReport[i];
    }

    return report;
  }
}