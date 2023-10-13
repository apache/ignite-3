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

package org.apache.ignite.internal.placementdriver.event;

import org.apache.ignite.internal.event.Event;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;

/** Primary replica management events. */
public enum PrimaryReplicaEvent implements Event {
    /**
     * This event is fired when a primary replica is selected for a replication group.
     *
     * <p>The primary replica is a lease-based object, which means it is limited in time. Therefore, after receiving this event, it is
     * important to use {@link PlacementDriver#awaitPrimaryReplica} which will allow to work with this replica and understand when lease
     * expires or prolonged.</p>
     *
     * <p>Notes:</p>
     * <ul>
     *     <li>This event will fire strictly after the completion of the future from {@link PlacementDriver#awaitPrimaryReplica}.</li>
     *     <li>This event will fire in the metastore thread.</li>
     *     <li>If a lease prolongation occurs, this event will not fire.</li>
     *     <li>When working from a primary replica, it is recommended to check whether it has become outdated using
     *     {@link ReplicaMeta#getExpirationTime()}.</li>
     * </ul>
     */
    PRIMARY_REPLICA_ELECTED,

    /**
     * This event is fired when the primary replica lost its leasehold privileges.
     */
    PRIMARY_REPLICA_EXPIRED;
}
