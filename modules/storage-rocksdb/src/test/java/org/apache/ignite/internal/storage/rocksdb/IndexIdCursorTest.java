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

package org.apache.ignite.internal.storage.rocksdb;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbIndexes.indexPrefix;
import static org.apache.ignite.internal.storage.rocksdb.index.AbstractRocksDbIndexStorage.PREFIX_WITH_IDS_LENGTH;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.ByteUtils.intToBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.storage.rocksdb.IndexIdCursor.TableAndIndexId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

/** Class that contains tests for {@link IndexIdCursor}. */
class IndexIdCursorTest extends IgniteAbstractTest {
    private RocksDB rocksDb;

    @BeforeEach
    void setUp() throws RocksDBException {
        rocksDb = RocksDB.open(workDir.toString());
    }

    @AfterEach
    void tearDown() {
        RocksUtils.closeAll(rocksDb);
    }

    @Test
    void testEmptyCursor() {
        assertThat(getAll(null), is(empty()));
    }

    @Test
    void testSingleElement() throws RocksDBException {
        insertData(42, 123);

        assertThat(getAll(null), contains(123));
        assertThat(getAll(42), contains(123));
        assertThat(getAll(0), is(empty()));
    }

    @Test
    void testBorders() throws RocksDBException {
        insertData(0, Integer.MIN_VALUE);
        insertData(0, Integer.MAX_VALUE);
        insertData(0, -1);
        insertData(0, 0);

        // RocksDB uses unsigned comparison.
        assertThat(getAll(null), contains(0, Integer.MAX_VALUE, Integer.MIN_VALUE, -1));
    }

    @Test
    void testRemovesDuplicatesAndSorts() throws RocksDBException {
        for (int i = 5; i >= 0; i--) {
            for (short j = 0; j < 3; j++) {
                insertData(0, i, j);
            }
        }

        assertThat(getAll(null), contains(0, 1, 2, 3, 4, 5));
    }

    @Test
    void testTableFilter() throws RocksDBException {
        insertData(1, 2);
        insertData(0, 0);
        insertData(1, 3);
        insertData(0, 1);

        assertThat(getAll(null), contains(0, 1, 2, 3));
        assertThat(getAll(0), contains(0, 1));
        assertThat(getAll(1), contains(2, 3));
    }

    private List<Integer> getAll(@Nullable Integer tableId) {
        try (
                var readOptions = new ReadOptions();
                var upperBound = tableId == null || tableId == -1 ? null : new Slice(intToBytes(tableId + 1))
        ) {
            readOptions.setIterateUpperBound(upperBound);

            RocksIterator it = rocksDb.newIterator(readOptions);

            try (var cursor = new IndexIdCursor(it, tableId)) {
                return cursor.stream().map(TableAndIndexId::indexId).collect(toList());
            }
        }
    }

    private void insertData(int tableId, int indexId) throws RocksDBException {
        rocksDb.put(indexPrefix(tableId, indexId), BYTE_EMPTY_ARRAY);
    }

    private void insertData(int tableId, int indexId, short partitionId) throws RocksDBException {
        byte[] key = ByteBuffer.allocate(PREFIX_WITH_IDS_LENGTH)
                .putInt(tableId)
                .putInt(indexId)
                .putShort(partitionId)
                .array();

        rocksDb.put(key, BYTE_EMPTY_ARRAY);
    }
}
