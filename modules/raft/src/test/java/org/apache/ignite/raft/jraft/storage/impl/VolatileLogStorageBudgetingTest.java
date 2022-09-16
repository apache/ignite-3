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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VolatileLogStorageBudgetingTest {
    private final ControlledBudget budget = new ControlledBudget();

    private final LogStorage storage = new VolatileLogStorage(budget, new OnHeapLogs(), new OnHeapLogs());

    @BeforeEach
    void initStorage() {
        var logStorageOptions = new LogStorageOptions();
        logStorageOptions.setConfigurationManager(new ConfigurationManager());
        logStorageOptions.setLogEntryCodecFactory(LogEntryV1CodecFactory.getInstance());

        storage.init(logStorageOptions);
    }

    @Test
    void callsAppendEntryOnBudgetWhenBudgetAllowsToAppend() {
        budget.allowEntries = 1;

        storage.appendEntry(entry(1));

        assertThat(budget.appendedIndices, is(List.of(1L)));
    }

    @Test
    void callsAppendEntriesOnBudgetWhenBudgetAllowsToAppend() {
        budget.allowEntries = 2;

        storage.appendEntries(List.of(entry(1), entry(2)));

        assertThat(budget.appendedIndices, is(List.of(1L, 2L)));
    }

    private LogEntry entry(long index) {
        LogEntry entry = new LogEntry();
        entry.setId(new LogId(index, 1));
        return entry;
    }

    private static class ControlledBudget implements LogStorageBudget {
        private int allowEntries;

        private final List<Long> appendedIndices = new ArrayList<>();

        @Override
        public boolean hasRoomFor(LogEntry entry) {
            if (allowEntries > 0) {
                allowEntries--;

                return true;
            }

            return false;
        }

        @Override
        public void onAppended(LogEntry entry) {
            appendedIndices.add(entry.getId().getIndex());
        }

        @Override
        public void onAppended(List<LogEntry> entries) {
            entries.stream()
                    .map(LogEntry::getId)
                    .map(LogId::getIndex)
                    .forEach(appendedIndices::add);
        }
    }
}
