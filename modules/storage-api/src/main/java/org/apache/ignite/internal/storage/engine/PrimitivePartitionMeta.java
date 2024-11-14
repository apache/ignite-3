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

package org.apache.ignite.internal.storage.engine;

import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Partition meta containing values of 'primitive' types (which are the same for all representations of partition metadata).
 */
public class PrimitivePartitionMeta {
    private final long lastAppliedIndex;
    private final long lastAppliedTerm;
    private final long leaseStartTime;
    private final @Nullable UUID primaryReplicaNodeId;
    private final @Nullable String primaryReplicaNodeName;

    /** Constructor. */
    public PrimitivePartitionMeta(
            long lastAppliedIndex,
            long lastAppliedTerm,
            long leaseStartTime,
            @Nullable UUID primaryReplicaNodeId,
            @Nullable String primaryReplicaNodeName
    ) {
        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
        this.leaseStartTime = leaseStartTime;
        this.primaryReplicaNodeId = primaryReplicaNodeId;
        this.primaryReplicaNodeName = primaryReplicaNodeName;
    }

    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    public long lastAppliedTerm() {
        return lastAppliedTerm;
    }

    public long leaseStartTime() {
        return leaseStartTime;
    }

    public @Nullable UUID primaryReplicaNodeId() {
        return primaryReplicaNodeId;
    }

    public @Nullable String primaryReplicaNodeName() {
        return primaryReplicaNodeName;
    }
}
