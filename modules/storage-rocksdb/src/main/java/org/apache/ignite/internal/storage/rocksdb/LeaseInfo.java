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

package org.apache.ignite.internal.storage.rocksdb;

import java.util.UUID;

/** Represents information about partition lease. */
class LeaseInfo {
    private final long leaseStartTime;
    private final UUID primaryReplicaNodeId;
    private final String primaryReplicaNodeName;

    /** Constructor. */
    LeaseInfo(long leaseStartTime, UUID primaryReplicaNodeId, String primaryReplicaNodeName) {
        this.leaseStartTime = leaseStartTime;
        this.primaryReplicaNodeId = primaryReplicaNodeId;
        this.primaryReplicaNodeName = primaryReplicaNodeName;
    }

    public long leaseStartTime() {
        return leaseStartTime;
    }

    public UUID primaryReplicaNodeId() {
        return primaryReplicaNodeId;
    }

    public String primaryReplicaNodeName() {
        return primaryReplicaNodeName;
    }
}
