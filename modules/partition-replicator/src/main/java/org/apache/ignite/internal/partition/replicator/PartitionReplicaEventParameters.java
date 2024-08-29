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

import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.event.EventParameters;

/**
 * Parameters for the events about zone partition replicas produced by {@link PartitionReplicaLifecycleManager}.
 */
public class PartitionReplicaEventParameters implements EventParameters {
    /** Zone descriptor. */
    private final CatalogZoneDescriptor zoneDescriptor;

    /** Partition id. */
    private final int partitionId;

    /**
     * Constructor.
     *
     * @param zoneDescriptor Zone descriptor.
     * @param partitionId Partition id.
     */
    public PartitionReplicaEventParameters(CatalogZoneDescriptor zoneDescriptor, int partitionId) {
        this.zoneDescriptor = zoneDescriptor;
        this.partitionId = partitionId;
    }

    /**
     * Returns zone descriptor.
     *
     * @return Zone descriptor.
     */
    public CatalogZoneDescriptor zoneDescriptor() {
        return zoneDescriptor;
    }

    /**
     * Returns partition id.
     *
     * @return Partition id.
     */
    public int partitionId() {
        return partitionId;
    }
}

