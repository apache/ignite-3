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

package org.apache.ignite.internal.storage.pagememory.index;

import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.MAX_BINARY_TUPLE_INLINE_SIZE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.AbstractHashIndexStorageTest;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.impl.BinaryTupleRowSerializer;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;

/**
 * Base class for testing {@link HashIndexStorage} based on {@link PageMemory}.
 */
abstract class AbstractPageMemoryHashIndexStorageTest extends AbstractHashIndexStorageTest {
    protected int pageSize;

    /**
     * Initializes the internal structures needed for tests.
     *
     * <p>This method *MUST* always be called in either subclass' constructor or setUp method.
     */
    final void initialize(
            MvTableStorage tableStorage,
            int pageSize
    ) {
        this.pageSize = pageSize;

        initialize(tableStorage);
    }

    @Test
    void testWithStringsLargerThanMaximumInlineSize() {
        HashIndexStorage index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.STRING);
        var serializer = new BinaryTupleRowSerializer(indexDescriptor(index));

        IndexRow indexRow0 = createIndexRow(serializer, new RowId(TEST_PARTITION), 1, randomString(random, MAX_BINARY_TUPLE_INLINE_SIZE));
        IndexRow indexRow1 = createIndexRow(serializer, new RowId(TEST_PARTITION), 1, randomString(random, MAX_BINARY_TUPLE_INLINE_SIZE));

        put(index, indexRow0);
        put(index, indexRow1);

        assertThat(getAll(index, indexRow0), contains(indexRow0.rowId()));
        assertThat(getAll(index, indexRow1), contains(indexRow1.rowId()));

        assertThat(getAll(index, createIndexRow(serializer, new RowId(TEST_PARTITION), 1, "foo")), empty());
    }

    @Test
    void testFragmentedIndexColumns() {
        HashIndexStorage index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.STRING);
        var serializer = new BinaryTupleRowSerializer(indexDescriptor(index));

        String longString0 = randomString(random, pageSize * 2);
        String longString1 = randomString(random, pageSize * 2);

        IndexRow indexRow0 = createIndexRow(serializer, new RowId(TEST_PARTITION), 1, longString0);
        IndexRow indexRow1 = createIndexRow(serializer, new RowId(TEST_PARTITION), 1, longString1);

        put(index, indexRow0);
        put(index, indexRow1);

        assertThat(getAll(index, indexRow0), contains(indexRow0.rowId()));
        assertThat(getAll(index, indexRow1), contains(indexRow1.rowId()));

        assertThat(getAll(index, createIndexRow(serializer, new RowId(TEST_PARTITION), 1, "foo")), empty());
    }
}
