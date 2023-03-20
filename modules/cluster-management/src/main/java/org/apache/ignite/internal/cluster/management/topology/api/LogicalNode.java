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

package org.apache.ignite.internal.cluster.management.topology.api;

import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;

/**
 * Ssdasd.
 */
public class LogicalNode extends ClusterNode {

    private final String nodeAttributes;

    /**
     * Cdfd.
     *
     * @param id sdsd.
     * @param name sdsd.
     * @param address sds.
     * @param nodeAttributes sds.
     */
    public LogicalNode(String id, String name, NetworkAddress address, String nodeAttributes) {
        super(id, name, address);

        this.nodeAttributes = nodeAttributes;
    }

    /**
     * Cdfd.
     *
     * @param clusterNode dsd.
     * @param nodeAttributes sds.
     */
    public LogicalNode(ClusterNode clusterNode, String nodeAttributes) {
        super(clusterNode.id(), clusterNode.name(), clusterNode.address(), clusterNode.nodeMetadata());

        this.nodeAttributes = nodeAttributes;
    }

    /**
     *  DSad.
     *
     * @return sadad.
     */
    public String nodeAttributes() {
        return nodeAttributes;
    }
}
