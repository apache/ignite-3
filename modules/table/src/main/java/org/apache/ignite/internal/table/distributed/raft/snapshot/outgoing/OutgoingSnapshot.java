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

package org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.table.message.SnapshotMetaRequest;
import org.apache.ignite.internal.table.message.SnapshotMetaResponse;
import org.apache.ignite.internal.table.message.SnapshotMvDataRequest;
import org.apache.ignite.internal.table.message.SnapshotMvDataResponse;
import org.apache.ignite.internal.table.message.SnapshotTxDataRequest;
import org.apache.ignite.internal.table.message.SnapshotTxDataResponse;

/**
 * Outgoing snapshot.
 */
public class OutgoingSnapshot {
    /**
     * Reads a snapshot meta and returns a future with the response.
     *
     * @param metaRequest Meta request.
     */
    CompletableFuture<SnapshotMetaResponse> handleSnapshotMetaRequest(SnapshotMetaRequest metaRequest) {
        //TODO https://issues.apache.org/jira/browse/IGNITE-17262
        return null;
    }

    /**
     * Reads chunk of partition data and returns a future with the response.
     *
     * @param mvDataRequest Data request.
     */
    CompletableFuture<SnapshotMvDataResponse> handleSnapshotMvDataRequest(SnapshotMvDataRequest mvDataRequest) {
        //TODO https://issues.apache.org/jira/browse/IGNITE-17262
        return null;
    }

    /**
     * Reads chunk of TX states from partition and returns a future with the response.
     *
     * @param txDataRequest Data request.
     */
    CompletableFuture<SnapshotTxDataResponse> handleSnapshotTxDataRequest(SnapshotTxDataRequest txDataRequest) {
        //TODO https://issues.apache.org/jira/browse/IGNITE-17262
        return null;
    }
}
