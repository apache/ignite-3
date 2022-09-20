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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.junit.jupiter.api.Test;

class EntryCountBudgetTest {
    @Test
    void allowsAppendWhenThereIsRoom() {
        EntryCountBudget budget = new EntryCountBudget(1);

        assertTrue(budget.hasRoomFor(entry(1)));
    }

    private LogEntry entry(long index) {
        LogEntry entry = new LogEntry();
        entry.setId(new LogId(index, 1));
        return entry;
    }

    @Test
    void deniesAppendWhenThereIsNoRoom() {
        EntryCountBudget budget = new EntryCountBudget(0);

        assertFalse(budget.hasRoomFor(entry(1)));
    }

    @Test
    void onAppendedSingleDecreasesRoom() {
        EntryCountBudget budget = new EntryCountBudget(1);

        budget.onAppended(entry(1));

        assertFalse(budget.hasRoomFor(entry(2)));
    }

    @Test
    void onAppendedMultipleDecreasesRoom() {
        EntryCountBudget budget = new EntryCountBudget(2);

        budget.onAppended(List.of(entry(1), entry(2)));

        assertFalse(budget.hasRoomFor(entry(3)));
    }

    @Test
    void onTruncatedPrefixForStoredPrefixIncreasesRoom() {
        EntryCountBudget budget = new EntryCountBudget(2);

        budget.onAppended(List.of(entry(1), entry(2)));

        budget.onTruncatedPrefix(2);

        assertTrue(budget.hasRoomFor(entry(3)));
    }

    @Test
    void onTruncatedPrefixForNonStoredPrefixDoesNotIncreaseRoom() {
        EntryCountBudget budget = new EntryCountBudget(2);

        budget.onAppended(List.of(entry(1), entry(2)));

        budget.onTruncatedPrefix(2);

        budget.onAppended(List.of(entry(3)));

        budget.onTruncatedPrefix(2);

        assertFalse(budget.hasRoomFor(entry(4)));
    }

    @Test
    void onTruncatedSuffixForStoredPrefixIncreasesRoom() {
        EntryCountBudget budget = new EntryCountBudget(2);

        budget.onAppended(List.of(entry(1), entry(2)));

        budget.onTruncatedSuffix(1);

        assertTrue(budget.hasRoomFor(entry(2)));
    }

    @Test
    void onTruncatedSuffixForNonStoredPrefixDoesNotIncreaseRoom() {
        EntryCountBudget budget = new EntryCountBudget(2);

        budget.onAppended(List.of(entry(1), entry(2)));

        budget.onTruncatedSuffix(2);

        assertFalse(budget.hasRoomFor(entry(3)));
    }
}
