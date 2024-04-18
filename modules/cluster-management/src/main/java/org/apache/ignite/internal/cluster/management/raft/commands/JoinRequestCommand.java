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

package org.apache.ignite.internal.cluster.management.raft.commands;

import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessageGroup;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.raft.WriteCommand;

/**
 * Command sent by a node that intends to join a cluster. This command will trigger node validation.
 */
@Transferable(CmgMessageGroup.Commands.JOIN_REQUEST)
public interface JoinRequestCommand extends WriteCommand {
    /**
     * Returns the node that wants to join a cluster.
     */
    ClusterNodeMessage node();

    /**
     * Returns the version of the Ignite node.
     */
    String version();

    /**
     * Returns the cluster tag.
     */
    ClusterTag clusterTag();

    /**
     * Returns the version of the Ignite node.
     */
    default IgniteProductVersion igniteVersion() {
        return IgniteProductVersion.fromString(version());
    }
}
