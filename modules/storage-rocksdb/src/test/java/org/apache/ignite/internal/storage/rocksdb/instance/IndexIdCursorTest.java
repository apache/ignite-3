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

package org.apache.ignite.internal.storage.rocksdb.instance;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.createKey;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import java.util.List;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/** Class that contains tests for {@link IndexIdCursor}. */
class IndexIdCursorTest extends IgniteAbstractTest {
    private RocksDB rocksDb;

    @BeforeEach
    void setUp() throws RocksDBException {
        rocksDb = RocksDB.open(workDir.toString());
    }

    @AfterEach
    void tearDown() {
        rocksDb.close();
    }

    @Test
    void testEmptyCursor() {
        assertThat(getAll(), is(empty()));
    }

    @Test
    void testSingleElement() throws RocksDBException {
        insertData(0, 0);

        assertThat(getAll(), contains(0));
    }

    @Test
    void testBorders() throws RocksDBException {
        insertData(Integer.MIN_VALUE, 0);
        insertData(Integer.MAX_VALUE, 0);
        insertData(-1, 0);
        insertData(0, 0);

        // RocksDB uses unsigned comparison.
        assertThat(getAll(), contains(0, Integer.MAX_VALUE, Integer.MIN_VALUE, -1));
    }

    @Test
    void testRemovesDuplicatesAndSorts() throws RocksDBException {
        for (int i = 5; i >= 0; i--) {
            for (int j = 0; j < 3; j++) {
                insertData(i, j);
            }
        }

        assertThat(getAll(), contains(0, 1, 2, 3, 4, 5));
    }

    private List<Integer> getAll() {
        try (var cursor = new IndexIdCursor(rocksDb.newIterator())) {
            return cursor.stream().collect(toList());
        }
    }

    private void insertData(int indexId, int extra) throws RocksDBException {
        rocksDb.put(createKey(BYTE_EMPTY_ARRAY, indexId, extra), BYTE_EMPTY_ARRAY);
    }
}
