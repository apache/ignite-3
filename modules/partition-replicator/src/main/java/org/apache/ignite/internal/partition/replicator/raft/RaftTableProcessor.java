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

package org.apache.ignite.internal.partition.replicator.raft;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.jetbrains.annotations.Nullable;

/**
 * Processor of Raft commands targeted at a particular table.
 */
public interface RaftTableProcessor {
    /**
     * Processes a Raft command.
     *
     * @param command Command.
     * @param commandIndex Command index.
     * @param commandTerm Command term.
     * @param safeTimestamp Safe timestamp.
     * @return Result of the command processing.
     */
    CommandResult processCommand(
            WriteCommand command,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    );

    /**
     * Called when a new Raft configuration is committed.
     */
    void onConfigurationCommitted(
            RaftGroupConfiguration config,
            long lastAppliedIndex,
            long lastAppliedTerm
    );

    /**
     * Sets the processor state to be later used by the processor to get information about replica state.
     *
     * @param state State to set.
     */
    default void processorState(ReplicaStoppingState state) {
        // No-op.
    }

    /**
     * Sets the initial state of this processor when it gets added to a zone partition.
     *
     * @param config Initial Raft configuration.
     * @param leaseInfo Initial lease information.
     * @param lastAppliedIndex Current last applied index.
     * @param lastAppliedTerm Current last applied term.
     */
    void initialize(
            @Nullable RaftGroupConfiguration config,
            @Nullable LeaseInfo leaseInfo,
            long lastAppliedIndex,
            long lastAppliedTerm
    );

    /**
     * Returns the last applied Raft log index.
     */
    long lastAppliedIndex();

    /**
     * Returns the term of the last applied Raft index.
     */
    long lastAppliedTerm();

    /**
     * Sets the last applied Raft log index and term.
     */
    void lastApplied(long lastAppliedIndex, long lastAppliedTerm);

    /**
     * Issues a flush of the underlying storage.
     *
     * @return Future that will be completed when the flush is done.
     */
    CompletableFuture<Void> flushStorage();

    /**
     * Called when the processor is being shut down.
     */
    void onShutdown();
}
