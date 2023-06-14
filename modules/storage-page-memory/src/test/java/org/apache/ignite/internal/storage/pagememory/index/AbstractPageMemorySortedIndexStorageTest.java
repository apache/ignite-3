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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

import java.util.Random;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType.ColumnTypeSpec;
import org.apache.ignite.internal.schema.testutils.definition.index.SortedIndexDefinition;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.AbstractSortedIndexStorageTest;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.impl.BinaryTupleRowSerializer;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.BasePageMemoryStorageEngineConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Base class for testing {@link SortedIndexStorage} based on {@link PageMemory}.
 */
abstract class AbstractPageMemorySortedIndexStorageTest extends AbstractSortedIndexStorageTest {
    protected BasePageMemoryStorageEngineConfiguration<?, ?> baseEngineConfig;

    private final Random random = new Random();

    /**
     * Initializes the internal structures needed for tests.
     *
     * <p>This method *MUST* always be called in either subclass' constructor or setUp method.
     */
    final void initialize(
            MvTableStorage tableStorage,
            TablesConfiguration tablesCfg,
            BasePageMemoryStorageEngineConfiguration<?, ?> baseEngineConfig
    ) {
        this.baseEngineConfig = baseEngineConfig;

        initialize(tableStorage, tablesCfg);
    }

    @Test
    void testWithStringsLargerThanMaximumInlineSize() throws Exception {
        SortedIndexDefinition indexDefinition = SchemaBuilders.sortedIndex("TEST_INDEX")
                .addIndexColumn(ColumnTypeSpec.INT32.name()).asc().done()
                .addIndexColumn(ColumnTypeSpec.STRING.name()).asc().done()
                .build();

        SortedIndexStorage index = createIndexStorage(indexDefinition);

        var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

        IndexRow indexRow0 = createIndexRow(serializer, new RowId(TEST_PARTITION), 10, randomString(random, MAX_BINARY_TUPLE_INLINE_SIZE));
        IndexRow indexRow1 = createIndexRow(serializer, new RowId(TEST_PARTITION), 10, randomString(random, MAX_BINARY_TUPLE_INLINE_SIZE));
        IndexRow indexRow2 = createIndexRow(serializer, new RowId(TEST_PARTITION), 20, randomString(random, MAX_BINARY_TUPLE_INLINE_SIZE));
        IndexRow indexRow3 = createIndexRow(serializer, new RowId(TEST_PARTITION), 20, randomString(random, MAX_BINARY_TUPLE_INLINE_SIZE));

        put(index, indexRow0);
        put(index, indexRow1);
        put(index, indexRow2);

        assertThat(get(index, indexRow0.indexColumns()), containsInAnyOrder(indexRow0.rowId()));
        assertThat(get(index, indexRow1.indexColumns()), containsInAnyOrder(indexRow1.rowId()));
        assertThat(get(index, indexRow2.indexColumns()), containsInAnyOrder(indexRow2.rowId()));

        assertThat(get(index, indexRow3.indexColumns()), empty());
    }

    @Test
    void testFragmentedIndexColumns() throws Exception {
        SortedIndexDefinition indexDefinition = SchemaBuilders.sortedIndex("TEST_INDEX")
                .addIndexColumn(ColumnTypeSpec.INT32.name()).asc().done()
                .addIndexColumn(ColumnTypeSpec.STRING.name()).asc().done()
                .build();

        SortedIndexStorage index = createIndexStorage(indexDefinition);

        var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

        int pageSize = baseEngineConfig.pageSize().value();

        IndexRow indexRow0 = createIndexRow(serializer, new RowId(TEST_PARTITION), 10, randomString(random, pageSize * 2));
        IndexRow indexRow1 = createIndexRow(serializer, new RowId(TEST_PARTITION), 10, randomString(random, pageSize * 2));
        IndexRow indexRow2 = createIndexRow(serializer, new RowId(TEST_PARTITION), 20, randomString(random, pageSize * 2));
        IndexRow indexRow3 = createIndexRow(serializer, new RowId(TEST_PARTITION), 20, randomString(random, pageSize * 2));

        put(index, indexRow0);
        put(index, indexRow1);
        put(index, indexRow2);

        assertThat(get(index, indexRow0.indexColumns()), containsInAnyOrder(indexRow0.rowId()));
        assertThat(get(index, indexRow1.indexColumns()), containsInAnyOrder(indexRow1.rowId()));
        assertThat(get(index, indexRow2.indexColumns()), containsInAnyOrder(indexRow2.rowId()));

        assertThat(get(index, indexRow3.indexColumns()), empty());
    }

    private static IndexRow createIndexRow(BinaryTupleRowSerializer serializer, RowId rowId, Object... objects) {
        return serializer.serializeRow(objects, rowId);
    }
}
