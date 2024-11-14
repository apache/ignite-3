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
 * Partition metadata for {@link MvTableStorage#finishRebalancePartition(int, MvPartitionMeta)}.
 */
public class MvPartitionMeta extends PrimitivePartitionMeta {
    private final byte[] groupConfig;

    /** Constructor. */
    public MvPartitionMeta(
            long lastAppliedIndex,
            long lastAppliedTerm,
            byte[] groupConfig,
            long leaseStartTime,
            @Nullable UUID primaryReplicaNodeId,
            @Nullable String primaryReplicaNodeName
    ) {
        super(lastAppliedIndex, lastAppliedTerm, leaseStartTime, primaryReplicaNodeId, primaryReplicaNodeName);

        this.groupConfig = groupConfig;
    }

    /** Returns replication group config as bytes. */
    public byte[] groupConfig() {
        return groupConfig;
    }
}
