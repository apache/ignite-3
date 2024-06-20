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

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;

/**
 * Service that provides an ability to await and retrieve primary replicas and assignments for replication groups.
 *
 * <p>Notes: If during recovery, the component needs to perform actions depending on whether the primary replica for some replication group
 * is a local node, then it needs to use {@link #getPrimaryReplica(ReplicationGroupId, HybridTimestamp)}. Then compare the local node with
 * {@link ReplicaMeta#getLeaseholder()} and {@link ReplicaMeta#getLeaseholderId()} and make sure that it has not yet expired by
 * {@link ReplicaMeta#getExpirationTime()}. And only then can we consider that the local node is the primary replica for the requested
 * replication group.</p>
 */
public interface PlacementDriver extends LeasePlacementDriver, AssignmentsPlacementDriver {
}
