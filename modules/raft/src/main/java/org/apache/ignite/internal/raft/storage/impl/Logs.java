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

package org.apache.ignite.internal.raft.storage.impl;

import java.util.List;
import org.apache.ignite.raft.jraft.Lifecycle;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.storage.Storage;

/**
 * Log entry storage (used for internal needs of {@link VolatileLogStorage}).
 */
interface Logs extends Lifecycle<LogStorageOptions>, Storage {
    /**
     * Gets logEntry by index.
     */
    LogEntry getEntry(long index);

    /**
     * Appends entries to log.
     */
    void appendEntry(LogEntry entry);

    /**
     * Appends entries to log.
     */
    void appendEntries(List<LogEntry> entries);

    /**
     * Deletes logs from storage's head, [first_log_index, first_index_kept) will be discarded.
     */
    void truncatePrefix(long firstIndexKept);

    /**
     * Deletes uncommitted logs from storage's tail, (last_index_kept, last_log_index] will be discarded.
     */
    void truncateSuffix(long lastIndexKept);

    /**
     * Drops all the existing logs and reset next log index to |next_log_index|. This function is called after installing
     * snapshot from leader.
     */
    void reset();
}
