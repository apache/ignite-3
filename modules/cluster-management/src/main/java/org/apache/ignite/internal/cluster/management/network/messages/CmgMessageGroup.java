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

package org.apache.ignite.internal.cluster.management.network.messages;

import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.raft.commands.ClusterNodeMessage;
import org.apache.ignite.internal.cluster.management.raft.commands.InitCmgStateCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinRequestCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.NodesLeaveCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.ReadLogicalTopologyCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.ReadStateCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.ReadValidatedNodesCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.UpdateClusterStateCommand;
import org.apache.ignite.internal.network.annotations.MessageGroup;

/**
 * Message Group for cluster initialization and CMG management.
 */
@MessageGroup(groupType = 7, groupName = "CmgMessages")
public class CmgMessageGroup {
    /**
     * Message type for {@link CmgInitMessage}.
     */
    public static final short CMG_INIT = 1;

    /**
     * Message type for {@link ClusterStateMessage}.
     */
    public static final short CLUSTER_STATE = 2;

    /**
     * Message type for {@link InitCompleteMessage}.
     */
    public static final short INIT_COMPLETE = 3;

    /**
     * Message type for {@link InitErrorMessage}.
     */
    public static final short INIT_ERROR = 4;

    /**
     * Message type for {@link CancelInitMessage}.
     */
    public static final short CANCEL_INIT = 5;

    /**
     * Message type for {@link SuccessResponseMessage}.
     */
    public static final short SUCCESS_RESPONSE = 6;

    /**
     * Message types for RAFT commands.
     */
    public interface Commands  {
        /**
         * Message type for {@link InitCmgStateCommand}.
         */
        int INIT_CMG_STATE = 40;

        /**
         * Message type for {@link ReadStateCommand}.
         */
        int READ_STATE = 41;

        /**
         * Message type for {@link ReadLogicalTopologyCommand}.
         */
        int READ_LOGICAL_TOPOLOGY = 42;

        /**
         * Message type for {@link JoinRequestCommand}.
         */
        int JOIN_REQUEST = 43;

        /**
         * Message type for {@link JoinReadyCommand}.
         */
        int JOIN_READY = 44;

        /**
         * Message type for {@link NodesLeaveCommand}.
         */
        int NODES_LEAVE = 45;

        /**
         * Message type for {@link ReadValidatedNodesCommand}.
         */
        int READ_VALIDATED_NODES = 46;

        /**
         * Message type for {@link ClusterNodeMessage}.
         */
        int CLUSTER_NODE = 60;

        /**
         * Message type for {@link ClusterState}.
         */
        int CLUSTER_STATE = 61;

        /**
         * Message type for {@link ClusterTag}.
         */
        int CLUSTER_TAG = 62;

        /**
         * Message type of {@link UpdateClusterStateCommand}.
         */
        int UPDATE_CMG_STATE = 65;
    }
}
