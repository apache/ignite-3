/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.network.recovery;

import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.recovery.message.StaleNodeHandlingParams;
import org.jetbrains.annotations.Nullable;

class MyStaleNodeHandlingParams implements StaleNodeHandlingParams {
    private final TopologyService topologyService;

    MyStaleNodeHandlingParams(TopologyService topologyService) {
        this.topologyService = topologyService;
    }

    @Override
    public int physicalTopologySize() {
        return topologyService.allMembers().size();
    }

    @Override
    public @Nullable String minNodeName() {
        return topologyService.allMembers().stream().map(InternalClusterNode::name).min(String::compareTo).orElse(null);
    }

    @Override
    public long topologyVersion() {
        return topologyService.logicalTopologyVersion();
    }
}
