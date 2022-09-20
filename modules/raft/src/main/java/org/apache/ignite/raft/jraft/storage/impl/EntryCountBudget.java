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
 * {@link LogStorageBudget} that makes sure that no more entries than the provided limit is stored.
 *
 * <p>This is not thread safe as {@link LogStorageBudget} implementations do not need to be thread safe (because all budget methods
 * are called under locks).
 */
public class EntryCountBudget implements LogStorageBudget {

    private static final long NO_INDEX = -1;

    private final long entriesCountLimit;

    private long firstIndex = NO_INDEX;
    private long lastIndex = NO_INDEX;

    public EntryCountBudget(long entriesCountLimit) {
        this.entriesCountLimit = entriesCountLimit;
    }

    @Override
    public boolean hasRoomFor(LogEntry entry) {
        return storedEntries() < entriesCountLimit;
    }

    private long storedEntries() {
        if (firstIndex == NO_INDEX && lastIndex == NO_INDEX) {
            return 0;
        } else if (firstIndex != NO_INDEX && lastIndex != NO_INDEX) {
            return lastIndex - firstIndex + 1;
        } else {
            throw new IllegalStateException("Only one of firstIndex and lastIndex is initialized: " + firstIndex + " and " + lastIndex);
        }
    }

    @Override
    public void onAppended(LogEntry entry) {
        if (firstIndex == NO_INDEX) {
            firstIndex = entry.getId().getIndex();
        }
        lastIndex = entry.getId().getIndex();
    }

    @Override
    public void onAppended(List<LogEntry> entries) {
        if (firstIndex == NO_INDEX) {
            firstIndex = entries.get(0).getId().getIndex();
        }
        lastIndex = entries.get(entries.size() - 1).getId().getIndex();
    }

    @Override
    public void onTruncatedPrefix(long firstIndexKept) {
        if (firstIndexKept <= lastIndex) {
            firstIndex = firstIndexKept;
        } else {
            clean();
        }
    }

    @Override
    public void onTruncatedSuffix(long lastIndexKept) {
        if (lastIndexKept >= firstIndex) {
            lastIndex = lastIndexKept;
        } else {
            clean();
        }
    }

    @Override
    public void onReset() {
        clean();
    }

    private void clean() {
        firstIndex = NO_INDEX;
        lastIndex = NO_INDEX;
    }
}
