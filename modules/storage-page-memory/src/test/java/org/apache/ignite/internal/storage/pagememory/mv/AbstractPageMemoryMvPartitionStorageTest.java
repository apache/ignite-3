/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.stream.IntStream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.AbstractMvPartitionStorageTest;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base test for MV partition storages based on PageMemory.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
abstract class AbstractPageMemoryMvPartitionStorageTest<T extends AbstractPageMemoryMvPartitionStorage> extends
        AbstractMvPartitionStorageTest<T> {
    protected final PageIoRegistry ioRegistry = new PageIoRegistry();

    protected final BinaryRow binaryRow3 = binaryRow(key, new TestValue(22, "bar3"));

    {
        ioRegistry.loadFromServiceLoader();
    }

    @WorkDirectory
    protected Path workDir;

    /**
     * Returns page size in bytes.
     */
    abstract int pageSize();

    /** {@inheritDoc} */
    @Override
    protected int partitionId() {
        // 1 instead of the default 0 to make sure that we note cases when we forget to pass the partition ID (in which
        // case it will turn into 0).
        return 1;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("JUnit3StyleTestMethodInJUnit4Class")
    @Override
    public void testReadsFromEmpty() {
        // TODO: enable the test when https://issues.apache.org/jira/browse/IGNITE-17006 is implemented

        // Effectively, disable the test because it makes no sense for this kind of storage as attempts to read using random
        // pageIds will result in troubles.
    }

    @Test
    void abortOfInsertMakesRowIdInvalidForAddWrite() {
        RowId rowId = storage.insert(binaryRow, newTransactionId());
        storage.abortWrite(rowId);

        assertThrows(RowIdIsInvalidForModificationsException.class, () -> storage.addWrite(rowId, binaryRow2, txId));
    }

    @Test
    void abortOfInsertMakesRowIdInvalidForCommitWrite() {
        RowId rowId = storage.insert(binaryRow, newTransactionId());
        storage.abortWrite(rowId);

        assertThrows(RowIdIsInvalidForModificationsException.class, () -> storage.commitWrite(rowId, Timestamp.nextVersion()));
    }

    @Test
    void abortOfInsertMakesRowIdInvalidForAbortWrite() {
        RowId rowId = storage.insert(binaryRow, newTransactionId());
        storage.abortWrite(rowId);

        assertThrows(RowIdIsInvalidForModificationsException.class, () -> storage.abortWrite(rowId));
    }

    @Test
    void uncommittedMultiPageValuesAreReadSuccessfully() {
        BinaryRow longRow = rowStoredInFragments();
        LinkRowId rowId = storage.insert(longRow, txId);

        BinaryRow foundRow = storage.read(rowId, txId);

        assertRowMatches(foundRow, longRow);
    }

    private BinaryRow rowStoredInFragments() {
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

        LinkRowId rowId = storage.insert(longRow, txId);
        storage.commitWrite(rowId, Timestamp.nextVersion());

        BinaryRow foundRow = storage.read(rowId, Timestamp.nextVersion());

        assertRowMatches(foundRow, longRow);
    }

    @Test
    void uncommittedMultiPageValuesWorkWithScans() throws Exception {
        BinaryRow longRow = rowStoredInFragments();
        storage.insert(longRow, txId);

        try (Cursor<BinaryRow> cursor = storage.scan(row -> true, txId)) {
            BinaryRow foundRow = cursor.next();

            assertRowMatches(foundRow, longRow);
        }
    }

    @Test
    void committedMultiPageValuesWorkWithScans() throws Exception {
        BinaryRow longRow = rowStoredInFragments();

        LinkRowId rowId = storage.insert(longRow, txId);
        storage.commitWrite(rowId, Timestamp.nextVersion());

        try (Cursor<BinaryRow> cursor = storage.scan(row -> true, txId)) {
            BinaryRow foundRow = cursor.next();

            assertRowMatches(foundRow, longRow);
        }
    }

    @Test
    void readByTimestampWorksCorrectlyAfterCommitAndAbortFollowedByUncommittedWrite() {
        RowId rowId = commitAbortAndAddUncommitted();

        BinaryRow foundRow = storage.read(rowId, Timestamp.nextVersion());

        assertRowMatches(foundRow, binaryRow);
    }

    private RowId commitAbortAndAddUncommitted() {
        RowId rowId = storage.insert(binaryRow, txId);
        storage.commitWrite(rowId, Timestamp.nextVersion());

        storage.addWrite(rowId, binaryRow2, newTransactionId());
        storage.abortWrite(rowId);

        storage.addWrite(rowId, binaryRow3, newTransactionId());

        return rowId;
    }

    @Test
    void scanByTimestampWorksCorrectlyAfterCommitAndAbortFollowedByUncommittedWrite() throws Exception {
        commitAbortAndAddUncommitted();

        try (Cursor<BinaryRow> cursor = storage.scan(k -> true, Timestamp.nextVersion())) {
            BinaryRow foundRow = cursor.next();

            assertRowMatches(foundRow, binaryRow);

            assertFalse(cursor.hasNext());
        }
    }

    @Test
    void readByTimestampWorksCorrectlyIfNoUncommittedValueExists() {
        LinkRowId rowId = storage.insert(binaryRow, txId);

        BinaryRow foundRow = storage.read(rowId, Timestamp.nextVersion());

        assertThat(foundRow, is(nullValue()));
    }
}
