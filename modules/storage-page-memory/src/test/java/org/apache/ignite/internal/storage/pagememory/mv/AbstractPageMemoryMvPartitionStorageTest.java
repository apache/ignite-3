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

package org.apache.ignite.internal.storage.pagememory.mv;

import static java.util.stream.Collectors.joining;

import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.schema.TableRow;
import org.apache.ignite.internal.storage.AbstractMvPartitionStorageTest;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.RowId;
import org.junit.jupiter.api.Test;

/**
 * Base test for MV partition storages based on PageMemory.
 */
abstract class AbstractPageMemoryMvPartitionStorageTest extends AbstractMvPartitionStorageTest {
    protected final PageIoRegistry ioRegistry = new PageIoRegistry();

    {
        ioRegistry.loadFromServiceLoader();
    }

    /**
     * Returns page size in bytes.
     */
    abstract int pageSize();

    @Test
    void uncommittedMultiPageValuesAreReadSuccessfully() {
        TableRow longRow = rowStoredInFragments();

        RowId rowId = insert(longRow, txId);

        TableRow foundRow = read(rowId, HybridTimestamp.MAX_VALUE);

        assertRowMatches(foundRow, longRow);
    }

    private TableRow rowStoredInFragments() {
        int pageSize = pageSize();

        // A repetitive pattern of 19 different characters (19 is chosen as a prime number) to reduce probability of 'lucky' matches
        // hiding bugs.
        String pattern = IntStream.range(0, 20)
                .mapToObj(ch -> String.valueOf((char) ('a' + ch)))
                .collect(joining());

        TestValue value = new TestValue(1, pattern.repeat((int) (2.5 * pageSize / pattern.length())));
        return tableRow(key, value);
    }

    @Test
    void committedMultiPageValuesAreReadSuccessfully() {
        TableRow longRow = rowStoredInFragments();

        RowId rowId = insert(longRow, txId);

        commitWrite(rowId, clock.now());

        TableRow foundRow = read(rowId, clock.now());

        assertRowMatches(foundRow, longRow);
    }

    @Test
    void uncommittedMultiPageValuesWorkWithScans() {
        TableRow longRow = rowStoredInFragments();

        insert(longRow, txId);

        try (PartitionTimestampCursor cursor = storage.scan(HybridTimestamp.MAX_VALUE)) {
            TableRow foundRow = cursor.next().tableRow();

            assertRowMatches(foundRow, longRow);
        }
    }

    @Test
    void committedMultiPageValuesWorkWithScans() {
        TableRow longRow = rowStoredInFragments();

        RowId rowId = insert(longRow, txId);

        commitWrite(rowId, clock.now());

        try (PartitionTimestampCursor cursor = storage.scan(HybridTimestamp.MAX_VALUE)) {
            TableRow foundRow = cursor.next().tableRow();

            assertRowMatches(foundRow, longRow);
        }
    }
}
