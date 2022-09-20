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

package org.apache.ignite.raft.jraft.storage.impl;

import java.util.List;
import org.apache.ignite.raft.jraft.entity.LogEntry;

/**
 * Knows how to determine whether there is still room for more log entries.
 *
 * <p>All methods are called under locks, so implementations do not need to be thread-safe.
 */
public interface LogStorageBudget {
    /**
     * Returns {@code true} if there is room for the given entry, {@code false} otherwise.
     *
     * @param entry Entry that is tried to be appended.
     */
    boolean hasRoomFor(LogEntry entry);

    /**
     * Updates the budget by letting it know that an entry was appended to the log storage.
     *
     * @param entry Entry that was appended.
     */
    default void onAppended(LogEntry entry) {
        // no-op
    }

    /**
     * Updates the budget by letting it know that entries were appended to the log storage.
     *
     * @param entries Entries that were appended.
     */
    default void onAppended(List<LogEntry> entries) {
        // no-op
    }

    /**
     * Updates the budget by letting it know that log prefix was truncated.
     *
     * @param firstIndexKept First index that was retained in the log; all indices before it are truncated.
     */
    default void onTruncatedPrefix(long firstIndexKept) {
        // no-op
    }

    /**
     * Updates the budget by letting it know that log suffix was truncated.
     *
     * @param lastIndexKept Last index that was retained in the log; all indices after it are truncated.
     */
    default void onTruncatedSuffix(long lastIndexKept) {
        // no-op
    }

    /**
     * Updates the budget by letting it know that the log was reset.
     *
     * @see org.apache.ignite.raft.jraft.storage.LogStorage#reset(long)
     */
    default void onReset() {
        // no-op
    }
}
