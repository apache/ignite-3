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

package org.apache.ignite.internal.placementdriver;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.affinity.TokenizedAssignments;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;

/**
 * Service that provides an ability to await and retrieve assignments for replication groups.
 * It's not guaranteed that retrieved assignments are ready to process requests, meaning that corresponding replicas
 * may not be started yet, or may be stopped at the next tick. On the stable replication group, assignments however
 * may be interpreted as replica hosts to process the requests.
 */
public interface AssignmentsPlacementDriver {

    /**
     * Returns the future with either newest available tokenized assignments for the specified replication group id or {@code null} if
     * there are no such assignments. The future will be completed after clusterTime (meta storage safe time) will become greater or equal
     * to the clusterTimeToAwait parameter.
     *
     * @param replicationGroupId Replication group Id.
     * @param clusterTimeToAwait Cluster time to await.
     * @return Tokenized assignments.
     */
    CompletableFuture<TokenizedAssignments>
    getAssignments(ReplicationGroupId replicationGroupId, HybridTimestamp clusterTimeToAwait);
}
