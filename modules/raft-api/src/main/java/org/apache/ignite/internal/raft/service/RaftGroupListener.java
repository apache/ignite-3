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

package org.apache.ignite.internal.raft.service;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.Consumer;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;

/**
 * A listener for replication group events.
 */
public interface RaftGroupListener {
    /**
     * Exception type that should be used by {@link #onRead(Iterator)} and {@link #onWrite(Iterator)} to complete their closures, in cases
     * when these methods have been invoked on a listener that's already shut down.
     */
    class ShutdownException extends RuntimeException {
        private static final long serialVersionUID = 5354137682604995519L;
    }

    /**
     * The callback to apply read commands.
     *
     * <p>If the runtime exception is thrown during iteration all unprocessed read requests will be aborted with the STM exception.
     *
     * @param iterator Read command iterator.
     */
    void onRead(Iterator<CommandClosure<ReadCommand>> iterator);

    /**
     * The callback to apply write commands.
     *
     * <p>If the runtime exception is thrown during iteration, all entries starting from current iteration are considered unapplied, the
     * state machine is invalidated and raft node will go into error state (will no longer can be elected as a leader and process
     * replication commands).
     *
     * <p>At this point the next step is to fix the problem and restart the raft node.
     *
     * @param iterator Write command iterator.
     */
    void onWrite(Iterator<CommandClosure<WriteCommand>> iterator);

    /**
     * Called when a configuration is committed (that is, written to a majority of the group).
     *
     * @param config Configuration that was committed.
     * @param lastAppliedIndex Last applied index.
     * @param lastAppliedTerm Last applied term.
     */
    default void onConfigurationCommitted(
            RaftGroupConfiguration config,
            long lastAppliedIndex,
            long lastAppliedTerm
    ) {
        // No-op
    }

    /**
     * The callback to save a snapshot. The execution should be asynchronous to avoid blocking of STM updates.
     * But the snapshot coordinates (or copy-of-data-to-include-in-snapshot) must be taken synchronously before starting the asynchronous
     * snapshotting process.
     *
     * @param path    Snapshot directory to store data.
     * @param doneClo The closure to call on finish. Pass the not null exception if the snapshot has not been created or null on successful
     *                creation.
     */
    void onSnapshotSave(Path path, Consumer<Throwable> doneClo);

    /**
     * The callback to load a snapshot.
     *
     * @param path Snapshot directory.
     * @return {@code True} if the snapshot was loaded successfully.
     */
    boolean onSnapshotLoad(Path path);

    /**
     * Invoked once after a raft node has been shut down.
     */
    void onShutdown();

    /**
     * Invoked when the belonging node becomes the leader of the group.
     */
    default void onLeaderStart() {
        // No-op.
    }

    /**
     * Invoked when the belonging node stops being the leader of the group.
     */
    default void onLeaderStop() {
        // No-op.
    }
}
