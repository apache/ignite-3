/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cluster.management.raft.commands;

import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.client.WriteCommand;

/**
 * Command sent by a node that intends to join a cluster. This command will trigger node validation.
 */
public class JoinRequestCommand implements WriteCommand {
    private final ClusterNode node;

    /**
     * Creates a new command.
     *
     * @param node Node that wants to enter the logical topology.
     */
    public JoinRequestCommand(ClusterNode node) {
        this.node = node;
    }

    /**
     * Returns the node that wants to join a cluster.
     *
     * @return Node that wants to join a cluster.
     */
    public ClusterNode node() {
        return node;
    }
}
