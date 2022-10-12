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

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.MAX_BINARY_TUPLE_INLINE_SIZE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.AbstractHashIndexStorageTest;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.BasePageMemoryStorageEngineConfiguration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Base class for testing {@link HashIndexStorage} based on {@link PageMemory}.
 */
abstract class AbstractPageMemoryHashIndexStorageTest extends AbstractHashIndexStorageTest {
    protected BasePageMemoryStorageEngineConfiguration<?, ?> baseEngineConfig;

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

    //TODO IGNITE-17626 Enable the test.
    @Disabled
    @Override
    public void testDestroy() {
    }

    @Test
    void testWithHashCollisionStrings() {
        checkHashCollisionStrings("foo");
    }

    @Test
    void testWithStringsLargerThanMaximumInlineSize() {
        checkHashCollisionStrings(randomString(MAX_BINARY_TUPLE_INLINE_SIZE));
    }

    @Test
    void testFragmentedIndexColumns() {
        checkHashCollisionStrings(randomString(baseEngineConfig.pageSize().value() * 2));
    }

    private void checkHashCollisionStrings(String prefix) {
        String[] hashCollisionPairStrings = createHashCollisionPairStrings(prefix);

        IndexRow indexRow0 = createIndexRow(1, hashCollisionPairStrings[0], new RowId(TEST_PARTITION));
        IndexRow indexRow1 = createIndexRow(1, hashCollisionPairStrings[1], new RowId(TEST_PARTITION));

        put(indexRow0);
        put(indexRow1);

        assertThat(getAll(indexRow0), contains(indexRow0.rowId()));
        assertThat(getAll(indexRow1), contains(indexRow1.rowId()));
    }

    private static String[] createHashCollisionPairStrings(String prefix) {
        String s0 = prefix + "Aa";
        String s1 = prefix + "BB";

        assertEquals(s0.hashCode(), s1.hashCode(), "s0=" + s0 + ", s1=" + s1);
        assertNotEquals(s0, s1);

        return new String[]{s0, s1};
    }

    private static String randomString(int size) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        return IntStream.range(0, size)
                .map(i -> 'A' + random.nextInt('Z' - 'A'))
                .mapToObj(i -> String.valueOf((char) i))
                .collect(joining());
    }
}
