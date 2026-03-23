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
import static org.apache.ignite.internal.schema.BinaryRowMatcher.isRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.schema.BinaryRow;
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
        BinaryRow longRow = rowStoredInFragments();

        RowId rowId = insert(longRow, txId);

        BinaryRow foundRow = read(rowId, HybridTimestamp.MAX_VALUE);

        assertThat(foundRow, isRow(longRow));
    }

    protected BinaryRow rowStoredInFragments() {
        int pageSize = pageSize();

        // A repetitive pattern of 19 different characters (19 is chosen as a prime number) to reduce probability of 'lucky' matches
        // hiding bugs.
        String pattern = IntStream.range(0, 20)
                .mapToObj(ch -> String.valueOf((char) ('a' + ch)))
                .collect(joining());

        TestValue value = new TestValue(1, pattern.repeat((int) (2.5 * pageSize / pattern.length())));
        return binaryRow(key, value);
    }

    @Test
    void committedMultiPageValuesAreReadSuccessfully() {
        BinaryRow longRow = rowStoredInFragments();

        RowId rowId = insert(longRow, txId);

        commitWrite(rowId, clock.now(), txId);

        BinaryRow foundRow = read(rowId, clock.now());

        assertThat(foundRow, isRow(longRow));
    }

    @Test
    void uncommittedMultiPageValuesWorkWithScans() {
        BinaryRow longRow = rowStoredInFragments();

        insert(longRow, txId);

        try (PartitionTimestampCursor cursor = storage.scan(HybridTimestamp.MAX_VALUE)) {
            BinaryRow foundRow = cursor.next().binaryRow();

            assertThat(foundRow, isRow(longRow));
        }
    }

    @Test
    void committedMultiPageValuesWorkWithScans() {
        BinaryRow longRow = rowStoredInFragments();

        RowId rowId = insert(longRow, txId);

        commitWrite(rowId, clock.now(), txId);

        try (PartitionTimestampCursor cursor = storage.scan(HybridTimestamp.MAX_VALUE)) {
            BinaryRow foundRow = cursor.next().binaryRow();

            assertThat(foundRow, isRow(longRow));
        }
    }

    @Test
    void addWriteCommittedCreatesPlainRowVersion() {
        addWriteCommitted(ROW_ID, binaryRow, clock.now());

        RowVersion rowVersion = pageMemoryStorage().findVersionChain(
                ROW_ID,
                chain -> pageMemoryStorage().readRowVersion(chain.headLink(), ts -> false)
        );
        assertThat(rowVersion, is(notNullValue()));

        assertThat(rowVersion.getClass(), is(RowVersion.class));
    }

    protected AbstractPageMemoryMvPartitionStorage pageMemoryStorage() {
        return (AbstractPageMemoryMvPartitionStorage) storage;
    }
}
