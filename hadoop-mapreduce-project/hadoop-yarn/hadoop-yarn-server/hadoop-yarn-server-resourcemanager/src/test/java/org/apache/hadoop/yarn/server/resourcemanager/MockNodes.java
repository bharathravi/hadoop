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
import java.util.Map;

import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeResponse;

import com.google.common.collect.Lists;

/**
 * Test helper to generate mock nodes
 */
public class MockNodes {
  private static int NODE_ID = 0;
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  public static List<RMNode> newNodes(int racks, int nodesPerRack,
                                        Resource perNode) {
    List<RMNode> list = Lists.newArrayList();
    for (int i = 0; i < racks; ++i) {
      for (int j = 0; j < nodesPerRack; ++j) {
        list.add(newNodeInfo(i, perNode));
      }
    }
    return list;
  }

  public static NodeId newNodeID(String host, int port) {
    NodeId nid = recordFactory.newRecordInstance(NodeId.class);
    nid.setHost(host);
    nid.setPort(port);
    return nid;
  }

  public static Resource newResource(int mem) {
    Resource rs = recordFactory.newRecordInstance(Resource.class);
    rs.setMemory(mem);
    return rs;
  }

  public static Resource newUsedResource(Resource total) {
    Resource rs = recordFactory.newRecordInstance(Resource.class);
    rs.setMemory((int)(Math.random() * total.getMemory()));
    return rs;
  }

  public static Resource newAvailResource(Resource total, Resource used) {
    Resource rs = recordFactory.newRecordInstance(Resource.class);
    rs.setMemory(total.getMemory() - used.getMemory());
    return rs;
  }

  public static RMNode newNodeInfo(int rack, final Resource perNode) {
    final String rackName = "rack"+ rack;
    final int nid = NODE_ID++;
    final String hostName = "host"+ nid;
    final int port = 123;
    final NodeId nodeID = newNodeID(hostName, port);
    final String httpAddress = "localhost:0";
    final NodeHealthStatus nodeHealthStatus =
        recordFactory.newRecordInstance(NodeHealthStatus.class);
    final Resource used = newUsedResource(perNode);
    final Resource avail = newAvailResource(perNode, used);
    return new RMNode() {
      @Override
      public NodeId getNodeID() {
        return nodeID;
      }

      @Override
      public String getNodeAddress() {
        return hostName;
      }

      @Override
      public String getHttpAddress() {
        return httpAddress;
      }

      @Override
      public Resource getTotalCapability() {
        return perNode;
      }

      @Override
      public String getRackName() {
        return rackName;
      }

      @Override
      public Node getNode() {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public NodeHealthStatus getNodeHealthStatus() {
        return nodeHealthStatus;
      }

      @Override
      public int getCommandPort() {
        return nid;
      }

      @Override
      public int getHttpPort() {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public String getHostName() {
        return hostName;
      }

      @Override
      public RMNodeState getState() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public List<ApplicationId> pullAppsToCleanup() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public List<ContainerId> pullContainersToCleanUp() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public HeartbeatResponse getLastHeartBeatResponse() {
        // TODO Auto-generated method stub
        return null;
      }
    };
  }
}
