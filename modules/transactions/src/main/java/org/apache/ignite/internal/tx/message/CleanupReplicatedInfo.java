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

package org.apache.ignite.internal.tx.message;

import java.io.Serializable;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.replicator.TablePartitionId;

/**
 * The result of a replicated cleanup request.
 */
public class CleanupReplicatedInfo implements Serializable {

    private static final long serialVersionUID = -975001033274630774L;

    private final UUID txId;

    private final Collection<TablePartitionId> partitions;

    public CleanupReplicatedInfo(UUID txId, Collection<TablePartitionId> partitions) {
        this.txId = txId;
        this.partitions = partitions;
    }

    public UUID txId() {
        return txId;
    }

    public Collection<TablePartitionId> partitions() {
        return partitions;
    }
}
