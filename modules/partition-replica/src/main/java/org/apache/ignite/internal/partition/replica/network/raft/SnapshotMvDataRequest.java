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

package org.apache.ignite.internal.partition.replica.network.raft;

import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.partition.replica.network.PartitionReplicationMessageGroup;

/**
 * Snapshot partition data request message.
 */
@Transferable(PartitionReplicationMessageGroup.SNAPSHOT_MV_DATA_REQUEST)
public interface SnapshotMvDataRequest extends SnapshotRequestMessage {
    /**
     * How many bytes the receiver is willing to receive. This corresponds to the sum of byte representations of row
     * versions, so the overall size of the message might exceed the hint (due to metadata and other fields of row versions).
     *
     * @return Batch size hint.
     */
    long batchSizeHint();
}
