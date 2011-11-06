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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

public class NavBlock extends HtmlBlock {

  @Override public void render(Block html) {
    html.
      div("#nav").
        h3("Cluster").
        ul().
          li().a(url("cluster"), "About")._().
          li().a(url("nodes"), "Nodes")._().
          li().a(url("apps"), "Applications")._().
          li().a(url("scheduler"), "Scheduler")._()._().
        h3("Tools").
        ul().
          li().a("/conf", "Configuration")._().
          li().a("/logs", "Local logs")._().
          li().a("/stacks", "Server stacks")._().
          li().a("/metrics", "Server metrics")._()._()._().
      div("#themeswitcher")._();
  }
}
