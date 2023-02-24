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
import java.util.Collection;
import java.util.Set;
import org.apache.ignite.internal.cluster.management.network.auth.RestAuthentication;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessageGroup;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Represents the state of the CMG. It contains:
 * <ol>
 *     <li>Names of the nodes that host the Meta Storage.</li>
 *     <li>Names of the nodes that host the CMG.</li>
 *     <li>Ignite version of the nodes that comprise the cluster.</li>
 *     <li>Cluster tag, unique for a particular Ignite cluster.</li>
 * </ol>
 */
@Transferable(CmgMessageGroup.Commands.CLUSTER_STATE)
public interface ClusterState extends NetworkMessage, Serializable {
    /**
     * Returns node names that host the CMG.
     */
    Set<String> cmgNodes();

    /**
     * Returns node names that host the Meta Storage.
     */
    Set<String> metaStorageNodes();

    /**
     * Returns a version of Ignite nodes that comprise this cluster.
     */
    String version();

    /**
     * Returns a cluster tag.
     */
    ClusterTag clusterTag();

    /**
     * Returns a version of Ignite nodes that comprise this cluster.
     */
    default IgniteProductVersion igniteVersion() {
        return IgniteProductVersion.fromString(version());
    }

    /**
     * Returns a REST authentication configuration that should be applied.
     */
    RestAuthentication restAuthToApply();

    /**
     * Creates a new cluster state instance. Acts like a constructor replacement.
     *
     * @param msgFactory Message factory to instantiate builder.
     * @param cmgNodes Collection of CMG nodes.
     * @param msNodes Collection of Metastorage nodes.
     * @param igniteVersion Ignite product version.
     * @param clusterTag Cluster tag instance.
     * @return Cluster state instance.
     */
    static ClusterState clusterState(
            CmgMessagesFactory msgFactory,
            Collection<String> cmgNodes,
            Collection<String> msNodes,
            IgniteProductVersion igniteVersion,
            ClusterTag clusterTag
    ) {
        return clusterState(msgFactory, cmgNodes, msNodes, igniteVersion, clusterTag, null);
    }

    /**
     * Creates a new cluster state instance. Acts like a constructor replacement.
     *
     * @param msgFactory Message factory to instantiate builder.
     * @param cmgNodes Collection of CMG nodes.
     * @param msNodes Collection of Metastorage nodes.
     * @param igniteVersion Ignite product version.
     * @param clusterTag Cluster tag instance.
     * @param restAuthentication REST authentication configuration.
     * @return Cluster state instance.
     */
    static ClusterState clusterState(
            CmgMessagesFactory msgFactory,
            Collection<String> cmgNodes,
            Collection<String> msNodes,
            IgniteProductVersion igniteVersion,
            ClusterTag clusterTag,
            RestAuthentication restAuthentication
    ) {
        return msgFactory.clusterState()
                .cmgNodes(Set.copyOf(cmgNodes))
                .metaStorageNodes(Set.copyOf(msNodes))
                .version(igniteVersion.toString())
                .clusterTag(clusterTag)
                .restAuthToApply(restAuthentication)
                .build();
    }
}
