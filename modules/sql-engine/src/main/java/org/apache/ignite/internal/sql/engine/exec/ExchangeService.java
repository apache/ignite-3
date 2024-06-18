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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryTupleMessage;
import org.apache.ignite.internal.sql.engine.exec.rel.Inbox;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;
import org.jetbrains.annotations.Nullable;

/**
 * The interface describes a minimal but sufficient set of methods to make
 * the exchange of information between mailboxes work.
 *
 * @see MailboxRegistry
 * @see Outbox
 * @see Inbox
 */
public interface ExchangeService extends LifecycleAware {
    /**
     * Asynchronously sends a batch of data to the specified node.
     *
     * @param nodeName The name of the node to which the data will be sent.
     * @param queryId The ID of the query to which the data belongs.
     * @param fragmentId The ID of the fragment to which the data will be sent.
     * @param exchangeId The ID of the exchange through which the data will be sent.
     * @param batchId The ID of the batch to which the data belongs.
     * @param last Indicates whether this is the last batch of data to be sent.
     * @param rows The data to be sent.
     * @return A {@link CompletableFuture future} representing the result of operation,
     *      which completes when the data has been sent.
     */
    CompletableFuture<Void> sendBatch(String nodeName, UUID queryId, long fragmentId, long exchangeId, int batchId, boolean last,
            List<BinaryTupleMessage> rows);

    /**
     * Asynchronously requests data from the specified node.
     *
     * @param nodeName The name of the node from which the data will be requested.
     * @param queryId The ID of the query for which the data is being requested.
     * @param fragmentId The ID of the fragment from which the data will be requested.
     * @param exchangeId The ID of the exchange through which the data will be requested.
     * @param amountOfBatches The number of batches of data to request.
     * @param state The state to propagate to the remote node, or null if state is not changed or not required.
     * @return A {@link CompletableFuture future} representing the result of operation,
     *      which completes when the request message has been sent.
     */
    CompletableFuture<Void> request(String nodeName, UUID queryId, long fragmentId, long exchangeId, int amountOfBatches,
            @Nullable SharedState state);

    /**
     * Asynchronously sends an error message to the specified node.
     *
     * @param nodeName The name of the node to which the error will be sent.
     * @param queryId The ID of the query to which the error belongs.
     * @param fragmentId The ID of the fragment to which the error belongs.
     * @param error The error to send.
     * @return A {@link CompletableFuture future} representing the result of operation,
     *      which completes when the error message has been sent.
     */
    CompletableFuture<Void> sendError(String nodeName, UUID queryId, long fragmentId, Throwable error);
}
