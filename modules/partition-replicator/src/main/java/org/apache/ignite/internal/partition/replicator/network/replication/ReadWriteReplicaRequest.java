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

package org.apache.ignite.internal.partition.replicator.network.replication;

import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.network.annotations.Transient;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaRequest;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.apache.ignite.internal.replicator.message.ZonePartitionIdMessage;
import org.jetbrains.annotations.Nullable;

/** Read-write replica request. */
public interface ReadWriteReplicaRequest extends PrimaryReplicaRequest, TimestampAware {
    UUID transactionId();

    /**
     * Get the transaction coordinator inconsistent ID.
     *
     * @return Transaction coordinator inconsistent ID.
     */
    UUID coordinatorId();

    /**
     * Return {@code true} if this is a full transaction.
     */
    boolean full();

    /** Commit partition ID. */
    ZonePartitionIdMessage commitPartitionId();

    /**
     * Get write request flag.
     *
     * @return {@code true} if this is write request (requires replication layer).
     */
    default boolean isWrite() {
        return false;
    }

    /**
     * Get delayed ack processor.
     *
     * @return The processor.
     */
    @Transient
    @Nullable BiConsumer<Object, Throwable> delayedAckProcessor();

    /**
     * Disable delayed ack optimization.
     *
     * @return {@code True} to disable the delayed ack optimization.
     */
    boolean skipDelayedAck();

    /**
     * Transaction label.
     *
     * @return Transaction label.
     */
    @Nullable String txLabel();
}
