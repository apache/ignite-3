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

package org.apache.ignite.internal.storage.index;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.impl.BinaryTupleRowSerializer;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;

/**
 * Base class for Hash Index storage tests.
 */
public abstract class AbstractHashIndexStorageTest extends AbstractIndexStorageTest<HashIndexStorage, StorageHashIndexDescriptor> {
    @Override
    protected HashIndexStorage createIndexStorage(String name, boolean built, ColumnType... columnTypes) {
        CatalogTableDescriptor tableDescriptor = catalogService.table(TABLE_NAME, clock.nowLong());

        int tableId = tableDescriptor.id();
        int indexId = catalogId.getAndIncrement();

        CatalogHashIndexDescriptor indexDescriptor = createCatalogIndexDescriptor(tableId, indexId, name, columnTypes);

        HashIndexStorage indexStorage = tableStorage.getOrCreateHashIndex(
                TEST_PARTITION,
                new StorageHashIndexDescriptor(tableDescriptor, indexDescriptor)
        );

        if (built) {
            completeBuildIndex(indexStorage);
        }

        return indexStorage;
    }

    @Override
    protected StorageHashIndexDescriptor indexDescriptor(HashIndexStorage index) {
        return index.indexDescriptor();
    }

    @Override
    CatalogHashIndexDescriptor createCatalogIndexDescriptor(int tableId, int indexId, String indexName, ColumnType... columnTypes) {
        var indexDescriptor = new CatalogHashIndexDescriptor(
                indexId,
                indexName,
                tableId,
                false,
                AVAILABLE,
                catalogService.latestCatalogVersion(),
                0,
                Stream.of(columnTypes).map(AbstractIndexStorageTest::columnName).collect(toList())
        );

        addToCatalog(indexDescriptor);

        return indexDescriptor;
    }

    @Test
    public void testDestroy() {
        HashIndexStorage index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.STRING);

        int indexId = index.indexDescriptor().id();

        assertThat(tableStorage.getIndex(TEST_PARTITION, indexId), is(sameInstance(index)));

        var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

        IndexRow row1 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));
        IndexRow row2 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));
        IndexRow row3 = serializer.serializeRow(new Object[]{ 2, "bar" }, new RowId(TEST_PARTITION));

        put(index, row1);
        put(index, row2);
        put(index, row3);

        CompletableFuture<Void> destroyFuture = tableStorage.destroyIndex(index.indexDescriptor().id());

        assertThat(destroyFuture, willCompleteSuccessfully());

        assertThat(tableStorage.getIndex(TEST_PARTITION, indexId), is(nullValue()));

        index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.STRING);
        assertThat(getAll(index, row1), is(empty()));
        assertThat(getAll(index, row2), is(empty()));
        assertThat(getAll(index, row3), is(empty()));
    }

    protected IndexRow createIndexRow(BinaryTupleRowSerializer serializer, RowId rowId, Object... values) {
        return serializer.serializeRow(values, rowId);
    }
}
