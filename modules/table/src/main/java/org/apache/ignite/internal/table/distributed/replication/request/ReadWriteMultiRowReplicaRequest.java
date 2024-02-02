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

package org.apache.ignite.internal.table.distributed.replication.request;

import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Read-write multi-row replica request.
 */
@Transferable(TableMessageGroup.RW_MULTI_ROW_REPLICA_REQUEST)
public interface ReadWriteMultiRowReplicaRequest extends MultipleRowReplicaRequest, ReadWriteReplicaRequest, CommittableTxRequest {
    /**
     * Operation type for upsert.
     */
    byte OP_UPSERT = 0;

    /**
     * Operation type for delete.
     */
    byte OP_DELETE = 1;

    /**
     * Disable delayed ack optimization.
     *
     * @return {@code True} to disable the delayed ack optimization.
     */
    boolean skipDelayedAck();

    /**
     * A byte for every tuple in {@link #binaryTuples()} indicating an operation type for that row.
     * See {@link #OP_UPSERT} and {@link #OP_DELETE}.
     *
     * @return A byte for every tuple in {@link #binaryTuples()} indicating an operation type for that row.
     */
    byte[] binaryTuplesOperationTypes();
}
