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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFifoScheduler {
  private static final Log LOG = LogFactory.getLog(TestFifoScheduler.class);
  
  private ResourceManager resourceManager = null;
  
  @Before
  public void setUp() throws Exception {
    Store store = StoreFactory.getStore(new Configuration());
    resourceManager = new ResourceManager(store);
    resourceManager.init(new Configuration());
  }

  @After
  public void tearDown() throws Exception {
  }
  
  @Test
  public void test() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    int GB = 1024;
    MockNM nm1 = rm.registerNode("h1:1234", 6 * GB);
    MockNM nm2 = rm.registerNode("h2:5678", 4 * GB);

    RMApp app1 = rm.submitApp(2048);
    // kick the scheduling, 2 GB given to AM1, remaining 4GB on nm1
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    SchedulerNodeReport report_nm1 = rm.getResourceScheduler().getNodeReport(
        nm1.getNodeId());
    Assert.assertEquals(2 * GB, report_nm1.getUsedResource().getMemory());

    RMApp app2 = rm.submitApp(2048);
    // kick the scheduling, 2GB given to AM, remaining 2 GB on nm2
    nm2.nodeHeartbeat(true);
    RMAppAttempt attempt2 = app2.getCurrentAppAttempt();
    MockAM am2 = rm.sendAMLaunched(attempt2.getAppAttemptId());
    am2.registerAppAttempt();
    SchedulerNodeReport report_nm2 = rm.getResourceScheduler().getNodeReport(
        nm2.getNodeId());
    Assert.assertEquals(2 * GB, report_nm2.getUsedResource().getMemory());

    // add request for containers
    am1.addRequests(new String[] { "h1", "h2" }, GB, 1, 1);
    AMResponse am1Response = am1.schedule(); // send the request
    // add request for containers
    am2.addRequests(new String[] { "h1", "h2" }, 3 * GB, 0, 1);
    AMResponse am2Response = am2.schedule(); // send the request

    // kick the scheduler, 1 GB and 3 GB given to AM1 and AM2, remaining 0
    nm1.nodeHeartbeat(true);
    while (am1Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(1000);
      am1Response = am1.schedule();
    }
    while (am2Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 2...");
      Thread.sleep(1000);
      am2Response = am2.schedule();
    }
    // kick the scheduler, nothing given remaining 2 GB.
    nm2.nodeHeartbeat(true);

    List<Container> allocated1 = am1Response.getAllocatedContainers();
    Assert.assertEquals(1, allocated1.size());
    Assert.assertEquals(1 * GB, allocated1.get(0).getResource().getMemory());
    Assert.assertEquals(nm1.getNodeId(), allocated1.get(0).getNodeId());

    List<Container> allocated2 = am2Response.getAllocatedContainers();
    Assert.assertEquals(1, allocated2.size());
    Assert.assertEquals(3 * GB, allocated2.get(0).getResource().getMemory());
    Assert.assertEquals(nm1.getNodeId(), allocated2.get(0).getNodeId());
    
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    report_nm2 = rm.getResourceScheduler().getNodeReport(nm2.getNodeId());
    Assert.assertEquals(0, report_nm1.getAvailableResource().getMemory());
    Assert.assertEquals(2 * GB, report_nm2.getAvailableResource().getMemory());

    Assert.assertEquals(6 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(2 * GB, report_nm2.getUsedResource().getMemory());

    Container c1 = allocated1.get(0);
    Assert.assertEquals(GB, c1.getResource().getMemory());
    c1.setState(ContainerState.COMPLETE);
    nm1.containerStatus(c1);
    int waitCount = 0;
    while (attempt1.getJustFinishedContainers().size() < 1
        && waitCount++ != 20) {
      LOG.info("Waiting for containers to be finished for app 1... Tried "
          + waitCount + " times already..");
      Thread.sleep(1000);
    }
    Assert.assertEquals(1, attempt1.getJustFinishedContainers().size());
    Assert.assertEquals(1, am1.schedule().getCompletedContainersStatuses().size());
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(5 * GB, report_nm1.getUsedResource().getMemory());

    rm.stop();
  }

  public static void main(String[] args) throws Exception {
    TestFifoScheduler t = new TestFifoScheduler();
    t.test();
  }
}
