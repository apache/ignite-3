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

import static org.apache.ignite.raft.jraft.test.TestUtils.mockEntry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class VolatileLogStorageSpecificsTest extends BaseIgniteAbstractTest {
    private static final int TERM = 1;

    private VolatileLogStorage logStorage;

    @Mock
    private LogStorageBudget inMemoryBudget;

    @Spy
    private Logs inMemoryLogs = new OnHeapLogs();

    @Spy
    private Logs spiltToDisk = new OnHeapLogs();

    @Test
    void entrySpiltToDiskIsReadFromDisk() {
        createAndInitLogStorage(new EntryCountBudget(1));

        logStorage.appendEntry(testEntry(1));
        logStorage.appendEntry(testEntry(2));

        clearInvocations(inMemoryLogs, spiltToDisk);

        LogEntry entry1 = logStorage.getEntry(1);
        assertThat(entry1, is(notNullValue()));

        verify(inMemoryLogs, never()).getEntry(1);
        verify(spiltToDisk).getEntry(1);
    }

    private static LogEntry testEntry(int index) {
        return mockEntry(index, TERM);
    }

    private void createAndInitLogStorage(LogStorageBudget inMemoryBudget) {
        logStorage = new VolatileLogStorage(inMemoryBudget, inMemoryLogs, spiltToDisk);

        initLogStorage();
    }

    private void initLogStorage() {
        var logStorageOptions = new LogStorageOptions();

        logStorageOptions.setConfigurationManager(new ConfigurationManager());
        logStorageOptions.setLogEntryCodecFactory(LogEntryV1CodecFactory.getInstance());

        logStorage.init(logStorageOptions);
    }

    @Test
    void entryKeptInMemoryIsReadFromMemory() {
        createAndInitLogStorage(new UnlimitedBudget());

        logStorage.appendEntry(testEntry(1));

        LogEntry entry1 = logStorage.getEntry(1);
        assertThat(entry1, is(notNullValue()));

        verify(inMemoryLogs).getEntry(1);
        verify(spiltToDisk, never()).getEntry(1);
    }

    @Test
    void spillWorkdsSequentially() {
        createAndInitLogStorage(new EntryCountBudget(1));

        logStorage.appendEntry(testEntry(1));
        logStorage.appendEntry(testEntry(2));
        logStorage.appendEntry(testEntry(3));
        logStorage.appendEntry(testEntry(4));

        clearInvocations(inMemoryLogs, spiltToDisk);

        assertThat(logStorage.getEntry(1), is(notNullValue()));
        assertThat(logStorage.getEntry(2), is(notNullValue()));
        assertThat(logStorage.getEntry(3), is(notNullValue()));
        assertThat(logStorage.getEntry(4), is(notNullValue()));

        verify(spiltToDisk).getEntry(1);
        verify(spiltToDisk).getEntry(2);
        verify(spiltToDisk).getEntry(3);
        verify(inMemoryLogs).getEntry(4);

        verify(inMemoryLogs, never()).getEntry(1);
        verify(inMemoryLogs, never()).getEntry(2);
        verify(inMemoryLogs, never()).getEntry(3);
        verify(spiltToDisk, never()).getEntry(4);
    }

    @Test
    void entryTooBigToBeAllowedByBudgetAloneIsSuccessfullyAppendedAndRead() {
        when(inMemoryBudget.hasRoomFor(any())).thenReturn(false);
        createAndInitLogStorage(inMemoryBudget);

        logStorage.appendEntry(testEntry(1));

        LogEntry entry = logStorage.getEntry(1);

        assertThat(entry, is(notNullValue()));
        assertThat(entry.getId().getIndex(), is(1L));
    }

    @Test
    void entryTooBigToBeAllowedByBudgetAloneIsNeverPutToMemory() {
        when(inMemoryBudget.hasRoomFor(any())).thenReturn(false);
        createAndInitLogStorage(inMemoryBudget);

        logStorage.appendEntry(testEntry(1));

        verify(inMemoryLogs, never()).appendEntry(any());
        verify(inMemoryLogs, never()).appendEntries(any());
    }

    @Test
    void tooBigEntrySpillsAnyPreviousEntryOutToDisk() {
        AtomicBoolean budgetAllows = new AtomicBoolean(true);
        when(inMemoryBudget.hasRoomFor(any())).thenAnswer(invocation -> budgetAllows.get());
        createAndInitLogStorage(inMemoryBudget);

        logStorage.appendEntry(testEntry(1));
        logStorage.appendEntry(testEntry(2));
        logStorage.appendEntry(testEntry(3));

        budgetAllows.set(false);

        logStorage.appendEntry(testEntry(4));

        clearInvocations(inMemoryLogs, spiltToDisk);

        assertThat(logStorage.getEntry(1), is(notNullValue()));
        assertThat(logStorage.getEntry(2), is(notNullValue()));
        assertThat(logStorage.getEntry(3), is(notNullValue()));
        assertThat(logStorage.getEntry(4), is(notNullValue()));

        verify(spiltToDisk).getEntry(1);
        verify(spiltToDisk).getEntry(2);
        verify(spiltToDisk).getEntry(3);
        verify(spiltToDisk).getEntry(4);

        verify(inMemoryLogs, never()).getEntry(1);
        verify(inMemoryLogs, never()).getEntry(2);
        verify(inMemoryLogs, never()).getEntry(3);
        verify(inMemoryLogs, never()).getEntry(4);
    }

    @Test
    void truncatePrefixOnNonExistentPrefixDoesNothing() {
        createAndInitLogStorage(new UnlimitedBudget());

        logStorage.appendEntries(List.of(testEntry(1), testEntry(2)));

        logStorage.truncatePrefix(1);

        verify(inMemoryLogs, never()).truncatePrefix(anyLong());
        verify(spiltToDisk, never()).truncatePrefix(anyLong());
    }

    @Test
    void truncatePrefixOnNonSpiltStorageWorks() {
        createAndInitLogStorage(new UnlimitedBudget());

        logStorage.appendEntries(List.of(testEntry(1), testEntry(2)));

        logStorage.truncatePrefix(2);

        verify(inMemoryLogs).truncatePrefix(2);
        verify(spiltToDisk, never()).truncatePrefix(anyLong());
    }

    @Test
    void truncatePrefixResetsFirstAndLastIndicesIfStorageBecomesEmpty() {
        createAndInitLogStorage(new UnlimitedBudget());

        logStorage.appendEntry(testEntry(1));

        logStorage.truncatePrefix(2);

        assertThat(logStorage.getFirstLogIndex(), is(1L));
        assertThat(logStorage.getLastLogIndex(), is(0L));
    }

    @Test
    void truncatePrefixOnSpiltStorageWorks() {
        createAndInitLogStorage(new EntryCountBudget(1));

        logStorage.appendEntries(List.of(testEntry(1), testEntry(2)));

        clearInvocations(inMemoryLogs, spiltToDisk);

        logStorage.truncatePrefix(2);

        verify(inMemoryLogs).truncatePrefix(2);
        verify(spiltToDisk).truncatePrefix(2);
    }

    @Test
    void prefixTruncationFreesRoom() {
        createAndInitLogStorage(new EntryCountBudget(2));

        logStorage.appendEntries(List.of(testEntry(1), testEntry(2), testEntry(3)));

        logStorage.truncatePrefix(3);

        clearInvocations(inMemoryLogs, spiltToDisk);

        logStorage.appendEntry(testEntry(4));

        assertThat(logStorage.getEntry(3), is(notNullValue()));
        assertThat(logStorage.getEntry(4), is(notNullValue()));

        verify(inMemoryLogs).getEntry(3);
        verify(inMemoryLogs).getEntry(4);
    }

    @Test
    void truncateSuffixOnNonExistentPrefixDoesNothing() {
        createAndInitLogStorage(new UnlimitedBudget());

        logStorage.appendEntries(List.of(testEntry(1), testEntry(2)));

        logStorage.truncateSuffix(2);

        verify(inMemoryLogs, never()).truncateSuffix(anyLong());
        verify(spiltToDisk, never()).truncateSuffix(anyLong());
    }

    @Test
    void truncateSuffixOnNonSpiltStorageWorks() {
        createAndInitLogStorage(new UnlimitedBudget());

        logStorage.appendEntries(List.of(testEntry(1), testEntry(2)));

        logStorage.truncateSuffix(1);

        verify(inMemoryLogs).truncateSuffix(1);
        verify(spiltToDisk, never()).truncateSuffix(anyLong());
    }

    @Test
    void truncateSuffixResetsFirstAndLastIndicesIfStorageBecomesEmpty() {
        createAndInitLogStorage(new UnlimitedBudget());

        logStorage.appendEntry(testEntry(1));

        logStorage.truncateSuffix(0);

        assertThat(logStorage.getFirstLogIndex(), is(1L));
        assertThat(logStorage.getLastLogIndex(), is(0L));
    }

    @Test
    void truncateSuffixOnSpiltStorageWorks() {
        createAndInitLogStorage(new EntryCountBudget(1));

        logStorage.appendEntries(List.of(testEntry(1), testEntry(2), testEntry(3)));

        clearInvocations(inMemoryLogs, spiltToDisk);

        logStorage.truncateSuffix(1);

        verify(inMemoryLogs).truncateSuffix(1);
        verify(spiltToDisk).truncateSuffix(1);
    }

    @Test
    void suffixTruncationFreesRoom() {
        createAndInitLogStorage(new EntryCountBudget(2));

        logStorage.appendEntries(List.of(testEntry(1), testEntry(2), testEntry(3)));

        logStorage.truncateSuffix(2);

        clearInvocations(inMemoryLogs, spiltToDisk);

        logStorage.appendEntry(testEntry(3));

        assertThat(logStorage.getEntry(1), is(notNullValue()));
        assertThat(logStorage.getEntry(2), is(notNullValue()));
        assertThat(logStorage.getEntry(3), is(notNullValue()));

        verify(spiltToDisk).getEntry(1);
        verify(inMemoryLogs).getEntry(2);
        verify(inMemoryLogs).getEntry(3);
    }

    @Test
    void resetResetsUnderlyingLogs() {
        createAndInitLogStorage(new UnlimitedBudget());

        logStorage.appendEntries(List.of(testEntry(1), testEntry(2)));

        logStorage.reset(1);

        verify(inMemoryLogs).reset();
        verify(spiltToDisk).reset();
    }

    @Test
    void shutdownShutsDownInternalStorages() {
        createAndInitLogStorage(mock(LogStorageBudget.class));

        logStorage.shutdown();

        verify(inMemoryLogs).shutdown();
        verify(spiltToDisk).shutdown();
    }
}
