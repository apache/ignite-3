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

package org.apache.ignite.internal.cluster.management;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessageGroup;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.jetbrains.annotations.TestOnly;

/**
 * Cluster tag that is used to uniquely identify a cluster.
 *
 * <p>It consists of two parts: human-readable part (cluster name) that is provided by the init command and an auto-generated part
 * (cluster id).
 */
@Transferable(CmgMessageGroup.Commands.CLUSTER_TAG)
public interface ClusterTag extends NetworkMessage, Serializable {
    /** Auto-generated part. */
    UUID clusterId();

    /** Human-readable part. */
    String clusterName();

    /**
     * Creates a new cluster tag instance with auto-generated {@link #clusterId()}. Acts like a constructor replacement.
     *
     * @param msgFactory Message factory to instantiate builder.
     * @param name Cluster name.
     * @return Cluster tag instance.
     */
    @TestOnly
    static ClusterTag randomClusterTag(CmgMessagesFactory msgFactory, String name) {
        return msgFactory.clusterTag()
                .clusterName(name)
                .clusterId(UUID.randomUUID())
                .build();
    }

    /**
     * Creates a new cluster tag instance. Acts like a constructor replacement.
     *
     * @param msgFactory Message factory to instantiate builder.
     * @param name Cluster name.
     * @param clusterId Cluster ID.
     * @return Cluster tag instance.
     */
    static ClusterTag clusterTag(CmgMessagesFactory msgFactory, String name, UUID clusterId) {
        return msgFactory.clusterTag()
                .clusterName(name)
                .clusterId(clusterId)
                .build();
    }
}
