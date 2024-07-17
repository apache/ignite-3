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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.jetbrains.annotations.Nullable;

/**
 * Cleanup transaction message.
 */
@Transferable(TxMessageGroup.TX_CLEANUP_MSG)
public interface TxCleanupMessage extends TimestampAware {
    /**
     * Gets a transaction id to resolve.
     *
     * @return Transaction id.
     */
    UUID txId();

    /**
     * Returns replication groups aggregated by expected primary replica nodes.
     * Null when this message is sent at recovery.
     *
     * @return Replication groups aggregated by expected primary replica nodes.
     */
    @Nullable List<TablePartitionIdMessage> groups();

    /**
     * Returns {@code True} if a commit request.
     *
     * @return {@code True} to commit.
     */
    boolean commit();

    /** Transaction commit timestamp. */
    @Nullable HybridTimestamp commitTimestamp();
}
