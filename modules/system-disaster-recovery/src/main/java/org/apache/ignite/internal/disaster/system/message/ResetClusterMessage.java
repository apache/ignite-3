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

package org.apache.ignite.internal.disaster.system.message;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * Message for resetting the cluster.
 */
@Transferable(SystemDisasterRecoveryMessageGroup.RESET_CLUSTER)
public interface ResetClusterMessage extends NetworkMessage, Serializable {
    /**
     * Consistent IDs of nodes that will host the new CMG.
     */
    Set<String> cmgNodes();

    /**
     * Consistent IDs of nodes that host the Meta Storage.
     */
    Set<String> metaStorageNodes();

    /**
     * Name of the cluster that will be a part of the generated cluster tag.
     */
    String clusterName();

    /**
     * New ID of the cluster.
     */
    UUID clusterId();

    /**
     * IDs that the cluster had before (including the current incarnation by which this message is sent).
     */
    List<UUID> formerClusterIds();

    /**
     * Consistent ID of the node that conducts the cluster reset.
     */
    String conductor();
}
