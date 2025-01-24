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

package org.apache.ignite.internal.partition.replicator;

import org.apache.ignite.internal.event.EventParameters;
import org.apache.ignite.internal.replicator.ZonePartitionId;

/**
 * Parameters for the events about zone partition replicas produced by {@link PartitionReplicaLifecycleManager}.
 */
public class LocalPartitionReplicaEventParameters implements EventParameters {
    /** Zone partition id. */
    private final ZonePartitionId zonePartitionId;

    /** Event's revision. */
    private final long revision;

    /**
     * Constructor.
     *
     * @param zonePartitionId Zone partition id.
     * @param revision Event's revision.
     */
    public LocalPartitionReplicaEventParameters(ZonePartitionId zonePartitionId, long revision) {
        this.zonePartitionId = zonePartitionId;
        this.revision = revision;
    }

    /**
     * Returns zone partition id.
     *
     * @return Zone partition id.
     */
    public ZonePartitionId zonePartitionId() {
        return zonePartitionId;
    }

    /**
     * Returns event's revision.
     *
     * @return Event's revision.
     */
    public long revision() {
        return revision;
    }
}

