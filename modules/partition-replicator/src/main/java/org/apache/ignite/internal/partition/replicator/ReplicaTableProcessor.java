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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;

/**
 * Processor of replica requests targeted at a particular table.
 */
public interface ReplicaTableProcessor {
    /**
     * Processes replica request.
     *
     * @param request Replica request.
     * @param replicaPrimacy Replica primacy info.
     * @param senderId ID of the node that sent the request.
     * @return Future completed with the result of processing.
     */
    CompletableFuture<ReplicaResult> process(ReplicaRequest request, ReplicaPrimacy replicaPrimacy, UUID senderId);

    /** Callback on replica shutdown. */
    void onShutdown();

    /** Returns tracker of RW transactions operations. */
    TableTxRwOperationTracker txRwOperationTracker();

    // TODO: https://issues.apache.org/jira/browse/IGNITE-27405 as safe time should not depend on the table.
    /** Returns safe time tracker for the partition. */
    PendingComparableValuesTracker<HybridTimestamp, Void> safeTime();
}
