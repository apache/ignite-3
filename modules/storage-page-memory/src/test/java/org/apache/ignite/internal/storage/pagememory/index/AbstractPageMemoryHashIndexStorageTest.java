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

import java.util.Random;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.AbstractHashIndexStorageTest;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.BasePageMemoryStorageEngineConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Base class for testing {@link HashIndexStorage} based on {@link PageMemory}.
 */
abstract class AbstractPageMemoryHashIndexStorageTest extends AbstractHashIndexStorageTest {
    protected BasePageMemoryStorageEngineConfiguration<?, ?> baseEngineConfig;

    private final Random random = new Random();

    /**
     * Initializes the internal structures needed for tests.
     *
     * <p>This method *MUST* always be called in either subclass' constructor or setUp method.
     */
    protected final void initialize(
            MvTableStorage tableStorage,
            TablesConfiguration tablesCfg,
            BasePageMemoryStorageEngineConfiguration<?, ?> baseEngineConfig
    ) {
        this.baseEngineConfig = baseEngineConfig;

        initialize(tableStorage, tablesCfg);
    }

    @Test
    void testWithStringsLargerThanMaximumInlineSize() {
        IndexRow indexRow0 = createIndexRow(1, randomString(random, MAX_BINARY_TUPLE_INLINE_SIZE), new RowId(TEST_PARTITION));
        IndexRow indexRow1 = createIndexRow(1, randomString(random, MAX_BINARY_TUPLE_INLINE_SIZE), new RowId(TEST_PARTITION));

        put(indexRow0);
        put(indexRow1);

        assertThat(getAll(indexRow0), contains(indexRow0.rowId()));
        assertThat(getAll(indexRow1), contains(indexRow1.rowId()));

        assertThat(getAll(createIndexRow(1, "foo", new RowId(TEST_PARTITION))), empty());
    }

    @Test
    void testFragmentedIndexColumns() {
        IndexRow indexRow0 = createIndexRow(1, randomString(random, baseEngineConfig.pageSize().value() * 2), new RowId(TEST_PARTITION));
        IndexRow indexRow1 = createIndexRow(1, randomString(random, baseEngineConfig.pageSize().value() * 2), new RowId(TEST_PARTITION));

        put(indexRow0);
        put(indexRow1);

        assertThat(getAll(indexRow0), contains(indexRow0.rowId()));
        assertThat(getAll(indexRow1), contains(indexRow1.rowId()));

        assertThat(getAll(createIndexRow(1, "foo", new RowId(TEST_PARTITION))), empty());
    }
}
