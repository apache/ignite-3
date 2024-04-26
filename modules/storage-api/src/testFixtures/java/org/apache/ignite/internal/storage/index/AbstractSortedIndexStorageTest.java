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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_FIRST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.DESC_NULLS_LAST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.GREATER;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.GREATER_OR_EQUAL;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.LESS;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.LESS_OR_EQUAL;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.BinaryTupleRowSerializer;
import org.apache.ignite.internal.storage.index.impl.TestIndexRow;
import org.apache.ignite.internal.storage.index.impl.TestIndexRow.TestIndexPrefix;
import org.apache.ignite.internal.testframework.VariableSource;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.Matchers;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Base class for Sorted Index storage tests.
 */
public abstract class AbstractSortedIndexStorageTest extends AbstractIndexStorageTest<SortedIndexStorage, StorageSortedIndexDescriptor> {
    private static final IgniteLogger LOG = Loggers.forClass(AbstractSortedIndexStorageTest.class);

    @Override
    protected SortedIndexStorage createIndexStorage(String name, boolean built, ColumnType... columnTypes) {
        return createIndexStorage(name, built, toCatalogIndexColumnDescriptors(columnTypes));
    }

    /**
     * Creates a Sorted Index using the given columns.
     *
     * @param name Index name.
     * @param built {@code True} to create a built index, {@code false} if you need to build it later.
     * @param columns Columns.
     */
    protected SortedIndexStorage createIndexStorage(String name, boolean built, List<ColumnParams> columns) {
        return createIndexStorage(
                name,
                built,
                columns.stream()
                        .map(ColumnParams::name)
                        .map(columnName -> new CatalogIndexColumnDescriptor(
                                columnName,
                                random.nextBoolean() ? ASC_NULLS_FIRST : DESC_NULLS_LAST
                        ))
                        .toArray(CatalogIndexColumnDescriptor[]::new)
        );
    }

    /**
     * Creates a built Sorted Index using the given columns.
     *
     * @param name Index name.
     * @param columns Columns.
     * @see #completeBuildIndex
     */
    protected SortedIndexStorage createIndexStorage(String name, List<ColumnParams> columns) {
        return createIndexStorage(name, true, columns);
    }

    /**
     * Creates a Sorted Index using the given index definition.
     *
     * @param name Index name.
     * @param built {@code True} to create a built index, {@code false} if you need to build it later.
     * @param columns Columns.
     * @see #completeBuildIndex
     */
    protected SortedIndexStorage createIndexStorage(String name, boolean built, CatalogIndexColumnDescriptor... columns) {
        CatalogTableDescriptor tableDescriptor = catalogService.table(TABLE_NAME, clock.nowLong());

        int tableId = tableDescriptor.id();
        int indexId = catalogId.getAndIncrement();

        CatalogSortedIndexDescriptor indexDescriptor = createCatalogIndexDescriptor(tableId, indexId, name, built, columns);

        SortedIndexStorage indexStorage = tableStorage.getOrCreateSortedIndex(
                TEST_PARTITION,
                new StorageSortedIndexDescriptor(tableDescriptor, indexDescriptor)
        );

        if (built) {
            completeBuildIndexForStorageOnly(indexStorage);
        }

        return indexStorage;
    }

    /**
     * Creates a built Sorted Index using the given index definition.
     *
     * @param name Index name.
     * @param columns Columns.
     * @see #completeBuildIndex 
     */
    protected SortedIndexStorage createIndexStorage(String name, CatalogIndexColumnDescriptor... columns) {
        return createIndexStorage(name, true, columns);
    }

    @Override
    protected StorageSortedIndexDescriptor indexDescriptor(SortedIndexStorage index) {
        return index.indexDescriptor();
    }

    @Override
    CatalogSortedIndexDescriptor createCatalogIndexDescriptor(
            int tableId,
            int indexId,
            String indexName,
            boolean built,
            ColumnType... columnTypes
    ) {
        return createCatalogIndexDescriptor(tableId, indexId, indexName, built, toCatalogIndexColumnDescriptors(columnTypes));
    }

    private CatalogSortedIndexDescriptor createCatalogIndexDescriptor(
            int tableId,
            int indexId,
            String indexName,
            boolean built,
            CatalogIndexColumnDescriptor... columns
    ) {
        var indexDescriptor = new CatalogSortedIndexDescriptor(
                indexId,
                indexName,
                tableId,
                false,
                built ? AVAILABLE : REGISTERED,
                catalogService.latestCatalogVersion(),
                List.of(columns)
        );

        addToCatalog(indexDescriptor);

        return indexDescriptor;
    }

    /**
     * Tests that columns of all types are correctly serialized and deserialized.
     */
    @Test
    void testRowSerialization() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ALL_TYPES_COLUMN_PARAMS);

        Object[] columns = indexStorage.indexDescriptor().columns().stream()
                .map(StorageSortedIndexColumnDescriptor::type)
                .map(type -> SchemaTestUtils.generateRandomValue(random, type))
                .toArray();

        var serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        IndexRow row = serializer.serializeRow(columns, new RowId(TEST_PARTITION));

        Object[] actual = serializer.deserializeColumns(row);

        assertThat(actual, is(equalTo(columns)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEmpty(boolean readOnly) {
        SortedIndexStorage index = createIndexStorage(INDEX_NAME, shuffledRandomColumnParams());

        assertThat(scan(index, null, null, 0, readOnly), is(empty()));
    }

    /**
     * Tests the Put-Get-Remove case when an index is created using a single column.
     */
    @ParameterizedTest
    @VariableSource("ALL_TYPES_COLUMN_PARAMS")
    void testSingleColumnIndex(ColumnParams columnParms) {
        testPutGetRemove(List.of(columnParms));
    }

    /**
     * Tests that it is possible to add rows with the same columns but different Row IDs.
     */
    @Test
    void testMultiplePuts() {
        SortedIndexStorage index = createIndexStorage(INDEX_NAME, ColumnType.STRING, ColumnType.INT32);

        var columnValues1 = new Object[] { "foo", 1 };
        var columnValues2 = new Object[] { "bar", 3 };
        var rowId1 = new RowId(TEST_PARTITION);
        var rowId2 = new RowId(TEST_PARTITION);
        var rowId3 = new RowId(TEST_PARTITION);

        var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

        IndexRow row1 = serializer.serializeRow(columnValues1, rowId1);
        IndexRow row2 = serializer.serializeRow(columnValues1, rowId2);
        IndexRow row3 = serializer.serializeRow(columnValues2, rowId3);

        put(index, row1);
        put(index, row2);
        put(index, row3);

        List<Object[]> actualColumns = scan(index, null, null, 0, false);

        assertThat(actualColumns, contains(columnValues2, columnValues1, columnValues1));
    }

    /**
     * Tests the {@link SortedIndexStorage#remove} method.
     */
    @Test
    void testRemoveAndScan() {
        SortedIndexStorage index = createIndexStorage(INDEX_NAME, ColumnType.STRING, ColumnType.INT32);

        var columnValues1 = new Object[] { "foo", 1 };
        var columnValues2 = new Object[] { "bar", 3 };
        var rowId1 = new RowId(TEST_PARTITION);
        var rowId2 = new RowId(TEST_PARTITION);
        var rowId3 = new RowId(TEST_PARTITION);

        var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

        IndexRow row1 = serializer.serializeRow(columnValues1, rowId1);
        IndexRow row2 = serializer.serializeRow(columnValues1, rowId2);
        IndexRow row3 = serializer.serializeRow(columnValues2, rowId3);

        put(index, row1);
        put(index, row2);
        put(index, row3);

        List<Object[]> actualColumns = scan(index, null, null, 0, false);

        assertThat(actualColumns, contains(columnValues2, columnValues1, columnValues1));

        // Test that rows with the same indexed columns can be removed individually
        remove(index, row2);

        actualColumns = scan(index, null, null, 0, false);

        assertThat(actualColumns, contains(columnValues2, columnValues1));

        // Test that removing a non-existent row does nothing
        remove(index, row2);

        actualColumns = scan(index, null, null, 0, false);

        assertThat(actualColumns, contains(columnValues2, columnValues1));

        // Test that the first row can be actually removed
        remove(index, row1);

        actualColumns = scan(index, null, null, 0, false);

        assertThat(actualColumns, contains((Object) columnValues2));
    }

    /**
     * Tests the Put-Get-Remove case when an index is created using all possible column in random order.
     */
    @RepeatedTest(5)
    void testCreateMultiColumnIndex() {
        testPutGetRemove(shuffledColumnParams());
    }

    /**
     * Tests the happy case of the {@link SortedIndexStorage#scan} method.
     */
    @RepeatedTest(5)
    void testReadWriteScan() {
        testScan(false);
    }

    /**
     * Tests the happy case of the {@link SortedIndexStorage#readOnlyScan} method.
     */
    @RepeatedTest(5)
    void testReadOnlyScan() {
        testScan(true);
    }

    private void testScan(boolean readOnly) {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, shuffledColumnParams());

        List<TestIndexRow> entries = IntStream.range(0, 10)
                .mapToObj(i -> {
                    TestIndexRow entry = TestIndexRow.randomRow(indexStorage, TEST_PARTITION);

                    put(indexStorage, entry);

                    return entry;
                })
                .sorted()
                .collect(toList());

        int firstIndex = 3;
        int lastIndex = 8;

        TestIndexPrefix first = entries.get(firstIndex).prefix(3);
        TestIndexPrefix last = entries.get(lastIndex).prefix(5);

        List<IndexRow> expected = entries.stream()
                .filter(row -> row.compareTo(first) >= 0 && row.compareTo(last) <= 0)
                .collect(toList());

        assertThat(expected, hasSize(greaterThanOrEqualTo(lastIndex - firstIndex + 1)));

        try (Cursor<IndexRow> cursor = readOnly
                ? indexStorage.readOnlyScan(first.prefix(), last.prefix(), GREATER_OR_EQUAL | LESS_OR_EQUAL)
                : indexStorage.scan(first.prefix(), last.prefix(), GREATER_OR_EQUAL | LESS_OR_EQUAL)
        ) {
            List<IndexRow> actual = cursor.stream().collect(toList());

            assertThat(actual, hasSize(expected.size()));

            for (int i = firstIndex; i < actual.size(); ++i) {
                assertThat(actual.get(i).rowId(), is(equalTo(expected.get(i).rowId())));
            }
        }
    }

    @ParameterizedTest()
    @ValueSource(booleans = {true, false})
    public void testBoundsAndOrder(boolean readOnly) {
        ColumnType string = ColumnType.STRING;
        ColumnType int32 = ColumnType.INT32;

        SortedIndexStorage index1 = createIndexStorage(
                "TEST_INDEX_1",
                new CatalogIndexColumnDescriptor(columnName(string), ASC_NULLS_LAST),
                new CatalogIndexColumnDescriptor(columnName(int32), ASC_NULLS_LAST)
        );

        SortedIndexStorage index2 = createIndexStorage(
                "TEST_INDEX_2",
                new CatalogIndexColumnDescriptor(columnName(string), ASC_NULLS_LAST),
                new CatalogIndexColumnDescriptor(columnName(int32), DESC_NULLS_LAST)
        );

        Object[] val1090 = {"10", 90};
        Object[] val1080 = {"10", 80};
        Object[] val2090 = {"20", 90};
        Object[] val2080 = {"20", 80};

        for (SortedIndexStorage index : Arrays.asList(index1, index2)) {
            var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

            put(index, serializer.serializeRow(val1090, new RowId(TEST_PARTITION)));
            put(index, serializer.serializeRow(val1080, new RowId(TEST_PARTITION)));
            put(index, serializer.serializeRow(val2090, new RowId(TEST_PARTITION)));
            put(index, serializer.serializeRow(val2080, new RowId(TEST_PARTITION)));
        }

        // Test without bounds.
        assertThat(
                scan(index1, null, null, 0, readOnly),
                contains(val1080, val1090, val2080, val2090)
        );

        assertThat(
                scan(index2, null, null, 0, readOnly),
                contains(val1090, val1080, val2090, val2080)
        );

        // Lower bound exclusive.
        assertThat(
                scan(index1, prefix(index1, "10"), null, GREATER, readOnly),
                contains(val2080, val2090)
        );

        assertThat(
                scan(index2, prefix(index2, "10"), null, GREATER, readOnly),
                contains(val2090, val2080)
        );

        // Lower bound inclusive.
        assertThat(
                scan(index1, prefix(index1, "10"), null, GREATER_OR_EQUAL, readOnly),
                contains(val1080, val1090, val2080, val2090)
        );

        assertThat(
                scan(index2, prefix(index2, "10"), null, GREATER_OR_EQUAL, readOnly),
                contains(val1090, val1080, val2090, val2080)
        );

        // Upper bound exclusive.
        assertThat(
                scan(index1, null, prefix(index1, "20"), LESS, readOnly),
                contains(val1080, val1090)
        );

        assertThat(
                scan(index2, null, prefix(index2, "20"), LESS, readOnly),
                contains(val1090, val1080)
        );

        // Upper bound inclusive.
        assertThat(
                scan(index1, null, prefix(index1, "20"), LESS_OR_EQUAL, readOnly),
                contains(val1080, val1090, val2080, val2090)
        );

        assertThat(
                scan(index2, null, prefix(index2, "20"), LESS_OR_EQUAL, readOnly),
                contains(val1090, val1080, val2090, val2080)
        );
    }

    /**
     * Tests that an empty range is returned if {@link SortedIndexStorage#scan} and {@link SortedIndexStorage#readOnlyScan} methods
     * are called using overlapping keys.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEmptyRange(boolean readOnly) {
        List<ColumnParams> indexSchema = shuffledRandomColumnParams();

        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, indexSchema);

        TestIndexRow entry1 = TestIndexRow.randomRow(indexStorage, TEST_PARTITION);
        TestIndexRow entry2 = TestIndexRow.randomRow(indexStorage, TEST_PARTITION);

        if (entry2.compareTo(entry1) < 0) {
            TestIndexRow t = entry2;
            entry2 = entry1;
            entry1 = t;
        }

        put(indexStorage, entry1);
        put(indexStorage, entry2);

        try (Cursor<IndexRow> cursor = getIndexRowCursor(
                indexStorage,
                entry2.prefix(indexSchema.size()).prefix(),
                entry1.prefix(indexSchema.size()).prefix(),
                0,
                readOnly
        )) {
            assertThat(cursor.stream().collect(toList()), is(empty()));
        }
    }

    @ParameterizedTest
    @MethodSource("allTypesColumnParamsAndReadOnly")
    void testNullValues(ColumnParams columnParams, boolean readOnly) {
        SortedIndexStorage storage = createIndexStorage(INDEX_NAME, List.of(columnParams));

        TestIndexRow entry1 = TestIndexRow.randomRow(storage, TEST_PARTITION);

        Object[] nullArray = new Object[1];

        var serializer = new BinaryTupleRowSerializer(storage.indexDescriptor());

        IndexRow nullRow = serializer.serializeRow(nullArray, new RowId(TEST_PARTITION));

        var entry2 = new TestIndexRow(storage, serializer, nullRow, nullArray);

        put(storage, entry1);
        put(storage, entry2);

        if (entry1.compareTo(entry2) > 0) {
            TestIndexRow t = entry2;
            entry2 = entry1;
            entry1 = t;
        }

        try (Cursor<IndexRow> cursor = getIndexRowCursor(
                storage,
                entry1.prefix(1).prefix(),
                entry2.prefix(1).prefix(),
                GREATER_OR_EQUAL | LESS_OR_EQUAL,
                readOnly
        )) {
            assertThat(
                    cursor.stream().map(row -> row.indexColumns().byteBuffer()).collect(toList()),
                    contains(entry1.indexColumns().byteBuffer(), entry2.indexColumns().byteBuffer())
            );
        }
    }

    /**
     * Checks simple scenarios for a scanning cursor.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testScanSimple(boolean readOnly) {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        for (int i = 0; i < 5; i++) {
            put(indexStorage, serializer.serializeRow(new Object[]{i}, new RowId(TEST_PARTITION)));
        }

        // Checking without borders.
        assertThat(
                scan(indexStorage, null, null, 0, AbstractSortedIndexStorageTest::firstArrayElement, readOnly),
                contains(0, 1, 2, 3, 4)
        );

        // Let's check with borders.
        assertThat(
                scan(
                        indexStorage,
                        serializer.serializeRowPrefix(0),
                        serializer.serializeRowPrefix(4),
                        (GREATER_OR_EQUAL | LESS_OR_EQUAL),
                        AbstractSortedIndexStorageTest::firstArrayElement,
                        readOnly
                ),
                contains(0, 1, 2, 3, 4)
        );

        assertThat(
                scan(
                        indexStorage,
                        serializer.serializeRowPrefix(0),
                        serializer.serializeRowPrefix(4),
                        (GREATER_OR_EQUAL | LESS),
                        AbstractSortedIndexStorageTest::firstArrayElement,
                        readOnly
                ),
                contains(0, 1, 2, 3)
        );

        assertThat(
                scan(
                        indexStorage,
                        serializer.serializeRowPrefix(0),
                        serializer.serializeRowPrefix(4),
                        (GREATER | LESS_OR_EQUAL),
                        AbstractSortedIndexStorageTest::firstArrayElement,
                        readOnly
                ),
                contains(1, 2, 3, 4)
        );

        assertThat(
                scan(
                        indexStorage,
                        serializer.serializeRowPrefix(0),
                        serializer.serializeRowPrefix(4),
                        (GREATER | LESS),
                        AbstractSortedIndexStorageTest::firstArrayElement,
                        readOnly
                ),
                contains(1, 2, 3)
        );

        // Let's check only with the lower bound.
        assertThat(
                scan(
                        indexStorage,
                        serializer.serializeRowPrefix(1),
                        null,
                        (GREATER_OR_EQUAL | LESS_OR_EQUAL),
                        AbstractSortedIndexStorageTest::firstArrayElement,
                        readOnly
                ),
                contains(1, 2, 3, 4)
        );

        assertThat(
                scan(
                        indexStorage,
                        serializer.serializeRowPrefix(1),
                        null,
                        (GREATER_OR_EQUAL | LESS),
                        AbstractSortedIndexStorageTest::firstArrayElement,
                        readOnly
                ),
                contains(1, 2, 3, 4)
        );

        assertThat(
                scan(
                        indexStorage,
                        serializer.serializeRowPrefix(1),
                        null,
                        (GREATER | LESS_OR_EQUAL),
                        AbstractSortedIndexStorageTest::firstArrayElement,
                        readOnly
                ),
                contains(2, 3, 4)
        );

        assertThat(
                scan(
                        indexStorage,
                        serializer.serializeRowPrefix(1),
                        null,
                        (GREATER | LESS),
                        AbstractSortedIndexStorageTest::firstArrayElement,
                        readOnly
                ),
                contains(2, 3, 4)
        );

        // Let's check only with the upper bound.
        assertThat(
                scan(
                        indexStorage,
                        null,
                        serializer.serializeRowPrefix(3),
                        (GREATER_OR_EQUAL | LESS_OR_EQUAL),
                        AbstractSortedIndexStorageTest::firstArrayElement,
                        readOnly
                ),
                contains(0, 1, 2, 3)
        );

        assertThat(
                scan(
                        indexStorage,
                        null,
                        serializer.serializeRowPrefix(3),
                        (GREATER_OR_EQUAL | LESS),
                        AbstractSortedIndexStorageTest::firstArrayElement,
                        readOnly
                ),
                contains(0, 1, 2)
        );

        assertThat(
                scan(
                        indexStorage,
                        null,
                        serializer.serializeRowPrefix(3),
                        (GREATER | LESS_OR_EQUAL),
                        AbstractSortedIndexStorageTest::firstArrayElement,
                        readOnly
                ),
                contains(0, 1, 2, 3)
        );

        assertThat(
                scan(
                        indexStorage,
                        null,
                        serializer.serializeRowPrefix(3),
                        (GREATER | LESS),
                        AbstractSortedIndexStorageTest::firstArrayElement,
                        readOnly
                ),
                contains(0, 1, 2)
        );
    }

    @Test
    void testScanContractAddRowBeforeInvokeHasNext() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        Cursor<IndexRow> scan = indexStorage.scan(null, null, 0);

        // index  =  [0]
        // cursor = ^ with no cached row
        put(indexStorage, serializer.serializeRow(new Object[]{0}, new RowId(TEST_PARTITION)));

        // index  = [0]
        // cursor =    ^ with cached [0]
        assertTrue(scan.hasNext());
        // index  = [0]
        // cursor =    ^ with no cached row
        assertEquals(0, serializer.deserializeColumns(scan.next())[0]);

        assertFalse(scan.hasNext());
        assertThrows(NoSuchElementException.class, scan::next);
    }

    @Test
    void testReadOnlyScanContractUpdateAfterScan() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        // Put before scan, should be visible.
        put(indexStorage, serializer.serializeRow(new Object[]{0}, new RowId(TEST_PARTITION)));

        Cursor<IndexRow> scan = indexStorage.readOnlyScan(null, null, 0);

        // Put after scan, should not be visible.
        put(indexStorage, serializer.serializeRow(new Object[]{1}, new RowId(TEST_PARTITION)));

        assertEquals(0, serializer.deserializeColumns(scan.next())[0]);
        assertFalse(scan::hasNext);
    }

    @Test
    void testScanContractAddRowAfterInvokeHasNext() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        Cursor<IndexRow> scan = indexStorage.scan(null, null, 0);

        // index  =
        // cursor = ^ already finished
        assertFalse(scan.hasNext());

        // index  =  [0]
        // cursor = ^ already finished
        put(indexStorage, serializer.serializeRow(new Object[]{0}, new RowId(TEST_PARTITION)));

        // index  =  [0]
        // cursor = ^ already finished
        assertFalse(scan.hasNext());
        assertThrows(NoSuchElementException.class, scan::next);

        // index  =  [0]
        // cursor = ^ no cached row
        scan = indexStorage.scan(null, null, 0);

        // index  = [0]
        // cursor =    ^ with cached [0]
        assertTrue(scan.hasNext());
        // index  = [0]
        // cursor =    ^ with no cached row
        assertEquals(0, serializer.deserializeColumns(scan.next())[0]);

        // index  = [0] [1]
        // cursor =    ^ with no cached row
        put(indexStorage, serializer.serializeRow(new Object[]{1}, new RowId(TEST_PARTITION)));

        // index  = [0] [1]
        // cursor =        ^ with cached [1]
        assertTrue(scan.hasNext());
        // index  = [0] [1]
        // cursor =        ^ with no cached row
        assertEquals(1, serializer.deserializeColumns(scan.next())[0]);

        // index  = [0] [1]
        // cursor =        ^ already finished
        assertFalse(scan.hasNext());
        assertThrows(NoSuchElementException.class, scan::next);
    }

    @Test
    void testScanContractInvokeOnlyNext() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        Cursor<IndexRow> scan = indexStorage.scan(null, null, 0);

        // index  =
        // cursor = ^ with no cached row
        assertThrows(NoSuchElementException.class, scan::next);

        // index  =  [0]
        // cursor = ^ with no cached row
        put(indexStorage, serializer.serializeRow(new Object[]{0}, new RowId(TEST_PARTITION)));

        // index  = [0]
        // cursor =    ^ already finished
        assertThrows(NoSuchElementException.class, scan::next);

        // index  =  [0]
        // cursor = ^ no cached row
        scan = indexStorage.scan(null, null, 0);

        // index  = [0]
        // cursor =     ^ no cached row
        assertEquals(0, serializer.deserializeColumns(scan.next())[0]);

        assertThrows(NoSuchElementException.class, scan::next);
        assertThrows(NoSuchElementException.class, scan::next);
    }

    @Test
    void testScanContractAddRowsOnly() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        RowId rowId0 = new RowId(TEST_PARTITION, 0, 0);
        RowId rowId1 = new RowId(TEST_PARTITION, 0, 1);
        RowId rowId2 = new RowId(TEST_PARTITION, 1, 0);

        Cursor<IndexRow> scan = indexStorage.scan(null, null, 0);

        // index  =  [0, r1]
        // cursor = ^ with no cached row
        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId1));

        // index  = [0, r1]
        // cursor =        ^ with no cached row
        IndexRow nextRow = scan.next();

        assertEquals(0, serializer.deserializeColumns(nextRow)[0]);
        assertEquals(rowId1, nextRow.rowId());

        // index  = [0, r0] [0, r1] [0, r2]
        // cursor =                ^ with no cached row
        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId0));
        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId2));

        // index  = [0, r0] [0, r1] [0, r2]
        // cursor =                        ^ with cached [0, r2]
        assertTrue(scan.hasNext());

        // index  = [0, r0] [0, r1] [0, r2]
        // cursor =                        ^ with no cached row
        nextRow = scan.next();

        assertEquals(0, serializer.deserializeColumns(nextRow)[0]);
        assertEquals(rowId2, nextRow.rowId());

        // index  = [-1, r0] [0, r0] [0, r1] [0, r2] [1, r0]
        // cursor =                                 ^ with no cached row
        put(indexStorage, serializer.serializeRow(new Object[]{1}, rowId0));
        put(indexStorage, serializer.serializeRow(new Object[]{-1}, rowId0));

        // index  = [-1, r0] [0, r0] [0, r1] [0, r2] [1, r0]
        // cursor =                                         ^ with cached [1, r0]
        assertTrue(scan.hasNext());

        // index  = [-1, r0] [0, r0] [0, r1] [0, r2] [1, r0]
        // cursor =                                         ^ with no cached row
        nextRow = scan.next();

        assertEquals(1, serializer.deserializeColumns(nextRow)[0]);
        assertEquals(rowId0, nextRow.rowId());

        assertFalse(scan.hasNext());
        assertThrows(NoSuchElementException.class, scan::next);
    }

    @Test
    void testScanContractForFinishCursor() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        Cursor<IndexRow> scan = indexStorage.scan(null, null, 0);

        // index  =
        // cursor = ^ with no cached row
        assertFalse(scan.hasNext());
        assertThrows(NoSuchElementException.class, scan::next);

        // index  =  [0]
        // cursor = ^ already finished
        put(indexStorage, serializer.serializeRow(new Object[]{0}, new RowId(TEST_PARTITION, 0, 0)));

        // index  =  [0]
        // cursor = ^ already finished
        assertFalse(scan.hasNext());
        assertThrows(NoSuchElementException.class, scan::next);

        scan = indexStorage.scan(null, null, 0);

        // index  = [0]
        // cursor =     ^ with cached [0, r0]
        assertTrue(scan.hasNext());
        // index  = [0]
        // cursor =    ^ with no cached row
        assertEquals(0, serializer.deserializeColumns(scan.next())[0]);

        // index  = [0]
        // cursor =    ^ already finished
        assertFalse(scan.hasNext());
        assertThrows(NoSuchElementException.class, scan::next);

        // index  = [-1] [0]
        // cursor =     ^ already finished
        put(indexStorage, serializer.serializeRow(new Object[]{-1}, new RowId(TEST_PARTITION, 0, 0)));

        // index  = [-1] [0]
        // cursor =     ^ already finished
        assertFalse(scan.hasNext());
        assertThrows(NoSuchElementException.class, scan::next);
    }

    @Test
    void testScanContractNextMethodOnly() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        RowId rowId0 = new RowId(TEST_PARTITION, 0, 0);
        RowId rowId1 = new RowId(TEST_PARTITION, 0, 1);
        RowId rowId2 = new RowId(TEST_PARTITION, 0, 1);

        Cursor<IndexRow> scan = indexStorage.scan(null, null, 0);

        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId0));

        // index  = [0, r0]
        // cursor =        ^ with no cached row
        IndexRow nextRow = scan.next();

        assertEquals(0, serializer.deserializeColumns(nextRow)[0]);
        assertEquals(rowId0, nextRow.rowId());

        // index  = [0, r0] [0, r1]
        // cursor =        ^ with no cached row
        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId1));

        // index  = [0, r0] [0, r1]
        // cursor =                ^ with no cached row
        nextRow = scan.next();

        assertEquals(0, serializer.deserializeColumns(nextRow)[0]);
        assertEquals(rowId1, nextRow.rowId());

        // index  = [-1, r2] [0, r0] [0, r1] [1, r2]
        // cursor =                         ^ with no cached row
        put(indexStorage, serializer.serializeRow(new Object[]{1}, rowId2));
        put(indexStorage, serializer.serializeRow(new Object[]{-1}, rowId2));

        // index  = [-1, r2] [0, r0] [0, r1] [1, r2]
        // cursor =                                 ^ with no cached row
        nextRow = scan.next();

        assertEquals(1, serializer.deserializeColumns(nextRow)[0]);
        assertEquals(rowId2, nextRow.rowId());

        assertThrows(NoSuchElementException.class, scan::next);
    }

    @Test
    void testScanContractRemoveRowsOnly() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        RowId rowId0 = new RowId(TEST_PARTITION, 0, 0);
        RowId rowId1 = new RowId(TEST_PARTITION, 0, 1);

        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId0));
        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId1));
        put(indexStorage, serializer.serializeRow(new Object[]{1}, rowId0));
        put(indexStorage, serializer.serializeRow(new Object[]{2}, rowId1));

        Cursor<IndexRow> scan = indexStorage.scan(null, null, 0);

        // index  = [0, r0] [0, r1] [1, r0] [2, r1]
        // cursor =        ^ with cached [0, r0]
        assertTrue(scan.hasNext());

        // index  =  [0, r1] [1, r0] [2, r1]
        // cursor = ^ with cached [0, r0]
        remove(indexStorage, serializer.serializeRow(new Object[]{0}, rowId0));

        // index  =  [0, r1] [1, r0] [2, r1]
        // cursor = ^ with no cached row
        IndexRow nextRow = scan.next();

        assertEquals(0, serializer.deserializeColumns(nextRow)[0]);
        assertEquals(rowId0, nextRow.rowId());

        // index  =  [1, r0] [2, r1]
        // cursor = ^ with no cached row
        remove(indexStorage, serializer.serializeRow(new Object[]{0}, rowId1));

        // index  = [1, r0] [2, r1]
        // cursor =        ^ with cached [1, r0]
        assertTrue(scan.hasNext());

        // index  = [1, r0] [2, r1]
        // cursor =        ^ with no cached row
        nextRow = scan.next();

        assertEquals(1, serializer.deserializeColumns(nextRow)[0]);
        assertEquals(rowId0, nextRow.rowId());

        // index  = [1, r0]
        // cursor =        ^ with no cached row
        remove(indexStorage, serializer.serializeRow(new Object[]{2}, rowId1));

        assertFalse(scan.hasNext());
        assertThrows(NoSuchElementException.class, scan::next);
    }

    @Test
    void testScanContractReplaceRow() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        RowId rowId = new RowId(TEST_PARTITION);

        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId));
        put(indexStorage, serializer.serializeRow(new Object[]{2}, rowId));

        Cursor<IndexRow> scan = indexStorage.scan(null, null, 0);

        // index  = [0] [2]
        // cursor =    ^ with no cached row
        assertEquals(0, serializer.deserializeColumns(scan.next())[0]);

        // Replace 0 -> 1.
        // index  =  [1] [2]
        // cursor = ^ with no cached row
        remove(indexStorage, serializer.serializeRow(new Object[]{0}, rowId));
        put(indexStorage, serializer.serializeRow(new Object[]{1}, rowId));

        // index  = [1] [2]
        // cursor =    ^ with cached [1]
        assertTrue(scan.hasNext());
        // index  = [1] [2]
        // cursor =    ^ with no cached row
        assertEquals(1, serializer.deserializeColumns(scan.next())[0]);

        // index  = [1] [2]
        // cursor =        ^ with cached [2]
        assertTrue(scan.hasNext());
        // index  = [1] [2]
        // cursor =        ^ with no cached row
        assertEquals(2, serializer.deserializeColumns(scan.next())[0]);

        assertFalse(scan.hasNext());
        assertThrows(NoSuchElementException.class, scan::next);
    }

    @Test
    void testScanContractRemoveCachedRow() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        Cursor<IndexRow> scan = indexStorage.scan(null, null, 0);

        RowId rowId = new RowId(TEST_PARTITION);

        // index  =  [0] [1]
        // cursor = ^ with no cached row
        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId));
        put(indexStorage, serializer.serializeRow(new Object[]{1}, rowId));

        // index  = [0] [1]
        // cursor =    ^ with cached [0]
        assertTrue(scan.hasNext());

        // index  =  [1]
        // cursor = ^ with cached [0]
        remove(indexStorage, serializer.serializeRow(new Object[]{0}, rowId));

        // index  =  [1]
        // cursor = ^ with no cached row
        assertEquals(0, serializer.deserializeColumns(scan.next())[0]);

        // index  = [1]
        // cursor =    ^ with cached [1]
        assertTrue(scan.hasNext());

        // index  =
        // cursor = ^ with cached [1]
        remove(indexStorage, serializer.serializeRow(new Object[]{1}, rowId));

        // index  =
        // cursor = ^ with no cached row
        assertEquals(1, serializer.deserializeColumns(scan.next())[0]);

        assertFalse(scan.hasNext());
        assertThrows(NoSuchElementException.class, scan::next);

        scan = indexStorage.scan(null, null, 0);

        // index  =  [2]
        // cursor = ^ with no cached row
        put(indexStorage, serializer.serializeRow(new Object[]{2}, rowId));

        // index  = [2]
        // cursor =    ^ with cached [2]
        assertTrue(scan.hasNext());

        // index  =
        // cursor = ^ with cached [2]
        remove(indexStorage, serializer.serializeRow(new Object[]{2}, rowId));

        // index  =
        // cursor = ^ with no cached row
        assertEquals(2, serializer.deserializeColumns(scan.next())[0]);

        assertFalse(scan.hasNext());
        assertThrows(NoSuchElementException.class, scan::next);
    }

    @Test
    void testScanContractRemoveNextAndAddFirstRow() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        RowId rowId = new RowId(TEST_PARTITION);

        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId));
        put(indexStorage, serializer.serializeRow(new Object[]{2}, rowId));

        Cursor<IndexRow> scan = indexStorage.scan(null, null, 0);

        // index  = [0] [2]
        // cursor =    ^ with no cached row
        assertEquals(0, serializer.deserializeColumns(scan.next())[0]);

        // index  = [-1] [2]
        // cursor =     ^ with no cached row
        remove(indexStorage, serializer.serializeRow(new Object[]{0}, rowId));
        put(indexStorage, serializer.serializeRow(new Object[]{-1}, rowId));

        // index  = [-1] [2]
        // cursor =         ^ with cached [2]
        assertTrue(scan.hasNext());
        // index  = [-1] [2]
        // cursor =         ^ with no cached row
        assertEquals(2, serializer.deserializeColumns(scan.next())[0]);

        assertFalse(scan.hasNext());
        assertThrows(NoSuchElementException.class, scan::next);
    }

    @Test
    void testScanPeekForFinishedCursor() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        PeekCursor<IndexRow> scan0 = indexStorage.scan(null, null, 0);
        PeekCursor<IndexRow> scan1 = indexStorage.scan(null, null, 0);

        // index   =
        // cursor0 = ^ already finished
        assertFalse(scan0.hasNext());
        assertNull(scan0.peek());

        // index   =
        // cursor1 = ^ already finished
        assertThrows(NoSuchElementException.class, scan1::next);
        assertNull(scan1.peek());

        // index   =  [0]
        // cursor0 = ^ already finished
        // cursor1 = ^ already finished
        put(indexStorage, serializer.serializeRow(new Object[]{0}, new RowId(TEST_PARTITION)));

        assertNull(scan0.peek());
        assertNull(scan1.peek());

        // index   =  [0]
        // cursor0 = ^ no cached row
        // cursor1 = ^ no cached row
        scan0 = indexStorage.scan(null, null, 0);
        scan1 = indexStorage.scan(null, null, 0);

        assertEquals(0, serializer.deserializeColumns(scan0.peek())[0]);
        assertEquals(0, serializer.deserializeColumns(scan1.peek())[0]);

        // index   = [0]
        // cursor0 =    ^ cached [0]
        assertTrue(scan0.hasNext());
        assertEquals(0, serializer.deserializeColumns(scan0.peek())[0]);

        // index   = [0]
        // cursor0 =    ^ no cached row
        assertEquals(0, serializer.deserializeColumns(scan0.next())[0]);
        assertNull(scan0.peek());

        // index   = [0]
        // cursor0 =    ^ already finished
        assertFalse(scan0.hasNext());
        assertThrows(NoSuchElementException.class, scan0::next);
        assertNull(scan0.peek());

        // index   = [0]
        // cursor1 =    ^ no cached row
        assertEquals(0, serializer.deserializeColumns(scan1.next())[0]);
        assertNull(scan1.peek());

        // index   = [0]
        // cursor1 =    ^ already finished
        assertThrows(NoSuchElementException.class, scan1::next);
        assertNull(scan1.peek());
    }

    @Test
    void testScanPeekAddRowsOnly() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        RowId rowId0 = new RowId(TEST_PARTITION, 0, 0);
        RowId rowId1 = new RowId(TEST_PARTITION, 0, 1);

        PeekCursor<IndexRow> scan = indexStorage.scan(null, null, 0);

        // index  =  [0, r1]
        // cursor = ^ no cached row
        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId1));

        assertEquals(SimpleRow.of(0, rowId1), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  =  [0, r0] [0, r1]
        // cursor = ^ no cached row
        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId0));

        assertEquals(SimpleRow.of(0, rowId0), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  = [0, r0] [0, r1]
        // cursor =        ^ cached [0, r0]
        assertTrue(scan.hasNext());

        assertEquals(SimpleRow.of(0, rowId0), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  = [-1, r0] [0, r0] [0, r1]
        // cursor =                 ^ cached [0, r0]
        put(indexStorage, serializer.serializeRow(new Object[]{-1}, rowId0));

        assertEquals(SimpleRow.of(0, rowId0), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  = [-1, r0] [0, r0] [0, r1] [1, r1]
        // cursor =                 ^ cached [0, r0]
        put(indexStorage, serializer.serializeRow(new Object[]{1}, rowId1));

        assertEquals(SimpleRow.of(0, rowId0), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  = [-1, r0] [0, r0] [0, r1] [1, r1]
        // cursor =                 ^ no cached row
        assertEquals(SimpleRow.of(0, rowId0), SimpleRow.of(scan.next(), firstColumn(serializer)));
        assertEquals(SimpleRow.of(0, rowId1), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  = [-1, r0] [0, r0] [0, r1] [1, r1]
        // cursor =                         ^ cached [0, r1]
        assertTrue(scan.hasNext());
        assertEquals(SimpleRow.of(0, rowId1), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  = [-1, r0] [0, r0] [0, r1] [1, r1]
        // cursor =                         ^ no cached row
        assertEquals(SimpleRow.of(0, rowId1), SimpleRow.of(scan.next(), firstColumn(serializer)));
        assertEquals(SimpleRow.of(1, rowId1), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  = [-1, r0] [0, r0] [0, r1] [1, r1]
        // cursor =                                 ^ cached [1, r1]
        assertTrue(scan.hasNext());
        assertEquals(SimpleRow.of(1, rowId1), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  = [-1, r0] [0, r0] [0, r1] [1, r1]
        // cursor =                                 ^ no cached row
        assertEquals(SimpleRow.of(1, rowId1), SimpleRow.of(scan.next(), firstColumn(serializer)));
        assertNull(scan.peek());

        // index  = [-1, r0] [0, r0] [0, r1] [1, r1]
        // cursor =                                 ^ already finished
        assertFalse(scan.hasNext());
        assertNull(scan.peek());

        // index  = [-1, r0] [0, r0] [0, r1] [1, r1]
        // cursor =                                 ^ already finished
        assertThrows(NoSuchElementException.class, scan::next);
        assertNull(scan.peek());
    }

    @Test
    void testScanPeekRemoveRows() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        RowId rowId0 = new RowId(TEST_PARTITION, 0, 0);
        RowId rowId1 = new RowId(TEST_PARTITION, 0, 1);

        PeekCursor<IndexRow> scan = indexStorage.scan(null, null, 0);

        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId0));
        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId1));
        put(indexStorage, serializer.serializeRow(new Object[]{1}, rowId0));
        put(indexStorage, serializer.serializeRow(new Object[]{2}, rowId1));

        // index  =  [0, r0] [0, r1] [1, r0] [2, r1]
        // cursor = ^ no cached row
        assertEquals(SimpleRow.of(0, rowId0), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  =  [0, r1] [1, r0] [2, r1]
        // cursor = ^ no cached row
        remove(indexStorage, serializer.serializeRow(new Object[]{0}, rowId0));

        // index  =  [0, r1] [1, r0] [2, r1]
        // cursor = ^ no cached row
        assertEquals(SimpleRow.of(0, rowId1), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  =  [1, r0] [2, r1]
        // cursor = ^ no cached row
        remove(indexStorage, serializer.serializeRow(new Object[]{0}, rowId1));

        assertEquals(SimpleRow.of(1, rowId0), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  = [1, r0] [2, r1]
        // cursor =        ^ cached [1, r0]
        assertTrue(scan.hasNext());
        assertEquals(SimpleRow.of(1, rowId0), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  = [1, r0]
        // cursor =        ^ cached [1, r0]
        remove(indexStorage, serializer.serializeRow(new Object[]{2}, rowId1));

        assertEquals(SimpleRow.of(1, rowId0), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        assertEquals(SimpleRow.of(1, rowId0), SimpleRow.of(scan.next(), firstColumn(serializer)));
        assertNull(scan.peek());

        // index  = [1, r0]
        // cursor =        ^ already finished
        assertFalse(scan.hasNext());
        assertNull(scan.peek());

        // index  = [1, r0]
        // cursor =        ^ already finished
        assertThrows(NoSuchElementException.class, scan::next);
        assertNull(scan.peek());
    }

    @Test
    void testScanPeekReplaceRow() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        PeekCursor<IndexRow> scan = indexStorage.scan(null, null, 0);

        RowId rowId = new RowId(TEST_PARTITION);

        // index  =  [0] [1]
        // cursor = ^ with no cached row
        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId));
        put(indexStorage, serializer.serializeRow(new Object[]{1}, rowId));

        // index  = [0] [1]
        // cursor =    ^ with cached [0]
        assertTrue(scan.hasNext());

        assertEquals(SimpleRow.of(0, rowId), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  = [0] [2]
        // cursor =    ^ with cached [0]
        remove(indexStorage, serializer.serializeRow(new Object[]{1}, rowId));
        put(indexStorage, serializer.serializeRow(new Object[]{2}, rowId));

        assertEquals(SimpleRow.of(0, rowId), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  = [0] [2]
        // cursor =    ^ with no cached row
        assertEquals(SimpleRow.of(0, rowId), SimpleRow.of(scan.next(), firstColumn(serializer)));
        assertEquals(SimpleRow.of(2, rowId), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  = [0] [2]
        // cursor =    ^ with cached [2]
        assertTrue(scan.hasNext());

        assertEquals(SimpleRow.of(2, rowId), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  = [0] [2]
        // cursor =        ^ with no cached row
        assertEquals(SimpleRow.of(2, rowId), SimpleRow.of(scan.next(), firstColumn(serializer)));

        // index  = [0] [2]
        // cursor =        ^ already finished
        assertFalse(scan.hasNext());
        assertNull(scan.peek());

        // index  = [0] [2]
        // cursor =        ^ already finished
        assertThrows(NoSuchElementException.class, scan::next);
        assertNull(scan.peek());
    }

    @Test
    void testScanPeekRemoveNext() {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, ColumnType.INT32);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        PeekCursor<IndexRow> scan = indexStorage.scan(null, null, 0);

        RowId rowId = new RowId(TEST_PARTITION);

        // index  =  [0]
        // cursor = ^ with no cached row
        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId));

        // index  =  [0]
        // cursor = ^ with no cached row
        assertEquals(SimpleRow.of(0, rowId), SimpleRow.of(scan.peek(), firstColumn(serializer)));

        // index  =
        // cursor = ^ with no cached row (but it remembers the last peek call)
        remove(indexStorage, serializer.serializeRow(new Object[]{0}, rowId));

        // "hasNext" and "next" must return the result of last "peek" operation. This is crucial for RW scans.
        assertTrue(scan.hasNext());
        assertEquals(SimpleRow.of(0, rowId), SimpleRow.of(scan.next(), firstColumn(serializer)));

        // index  =
        // cursor = ^ with no cached row
        assertNull(scan.peek());

        // index  =  [1]
        // cursor = ^ with no cached row (points before [1] because last returned value was [0])
        put(indexStorage, serializer.serializeRow(new Object[]{1}, rowId));

        // But, "hasNext" must return "false" to be consistent with the result of last "peek" operation. This is crucial for RW scans.
        assertFalse(scan.hasNext());
        assertThrows(NoSuchElementException.class, scan::next);
    }

    @Test
    public void testDestroy() {
        SortedIndexStorage index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.STRING);

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

        assertThat(tableStorage.getIndex(TEST_PARTITION, indexId), is(Matchers.nullValue()));

        index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.STRING);

        assertThat(getAll(index, row1), is(empty()));
        assertThat(getAll(index, row2), is(empty()));
        assertThat(getAll(index, row3), is(empty()));
    }

    @Test
    void testScanFromPkIndex() {
        SortedIndexStorage pkIndex = createPkIndexStorage();

        assertDoesNotThrow(() -> scan(pkIndex, index -> index.readOnlyScan(null, null, 0)));
        assertDoesNotThrow(() -> scan(pkIndex, index -> index.scan(null, null, 0)));
        assertDoesNotThrow(() -> scan(pkIndex, index -> index.tolerantScan(null, null, 0)));
    }

    @Test
    void testScanAfterBuiltIndex() {
        SortedIndexStorage index = createIndexStorage(INDEX_NAME, false, ColumnType.INT32);

        assertThrows(IndexNotBuiltException.class, () -> scan(index, i -> i.readOnlyScan(null, null, 0)));
        assertThrows(IndexNotBuiltException.class, () -> scan(index, i -> i.scan(null, null, 0)));
        assertDoesNotThrow(() -> scan(index, i -> i.tolerantScan(null, null, 0)));

        completeBuildIndex(index);

        assertDoesNotThrow(() -> scan(index, i -> i.readOnlyScan(null, null, 0)));
        assertDoesNotThrow(() -> scan(index, i -> i.scan(null, null, 0)));
        assertDoesNotThrow(() -> scan(index, i -> i.tolerantScan(null, null, 0)));
    }

    @Test
    void testTolerantScanAfterBuiltIndex() {
        SortedIndexStorage index = createIndexStorage(INDEX_NAME, false, ColumnType.INT32);

        completeBuildIndexInCatalogOnly(index);
        assertThrows(InconsistentIndexStateException.class, () -> scan(index, i -> i.tolerantScan(null, null, 0)));

        completeBuildIndexForStorageOnly(index);
        assertDoesNotThrow(() -> scan(index, i -> i.tolerantScan(null, null, 0)));
    }

    private List<ColumnParams> shuffledRandomColumnParams() {
        return shuffledColumnParams(d -> random.nextBoolean());
    }

    private List<ColumnParams> shuffledColumnParams() {
        return shuffledColumnParams(d -> true);
    }

    private List<ColumnParams> shuffledColumnParams(Predicate<ColumnParams> filter) {
        List<ColumnParams> shuffledDefinitions = ALL_TYPES_COLUMN_PARAMS.stream()
                .filter(filter)
                .collect(toList());

        if (shuffledDefinitions.isEmpty()) {
            shuffledDefinitions = new ArrayList<>(ALL_TYPES_COLUMN_PARAMS);
        }

        Collections.shuffle(shuffledDefinitions, random);

        if (LOG.isInfoEnabled()) {
            List<String> columnNames = shuffledDefinitions.stream().map(ColumnParams::name).collect(toList());

            LOG.info("Creating index with the following column order: " + columnNames);
        }

        return shuffledDefinitions;
    }

    /**
     * Tests the Get-Put-Remove scenario: inserts some keys into the storage and checks that they have been successfully persisted and can
     * be removed.
     */
    private void testPutGetRemove(List<ColumnParams> indexSchema) {
        SortedIndexStorage indexStorage = createIndexStorage(INDEX_NAME, indexSchema);

        TestIndexRow entry1 = TestIndexRow.randomRow(indexStorage, TEST_PARTITION);
        TestIndexRow entry2;

        // using a cycle here to protect against equal keys being generated
        do {
            entry2 = TestIndexRow.randomRow(indexStorage, TEST_PARTITION);
        } while (entry1.indexColumns().byteBuffer().equals(entry2.indexColumns().byteBuffer()));

        put(indexStorage, entry1);
        put(indexStorage, entry2);

        assertThat(
                getSingle(indexStorage, entry1.indexColumns()).rowId(),
                is(equalTo(entry1.rowId()))
        );

        assertThat(
                getSingle(indexStorage, entry2.indexColumns()).rowId(),
                is(equalTo(entry2.rowId()))
        );

        remove(indexStorage, entry1);

        assertThat(getSingle(indexStorage, entry1.indexColumns()), is(nullValue()));
    }

    private static BinaryTuplePrefix prefix(SortedIndexStorage index, Object... vals) {
        var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

        return serializer.serializeRowPrefix(vals);
    }

    private static List<Object[]> scan(
            SortedIndexStorage index,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            @MagicConstant(flagsFromClass = SortedIndexStorage.class) int flags,
            boolean readOnly
    ) {
        return scan(index, lowerBound, upperBound, flags, identity(), readOnly);
    }

    private static <T> List<T> scan(
            SortedIndexStorage storage,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            @MagicConstant(flagsFromClass = SortedIndexStorage.class) int flags,
            Function<Object[], T> mapper,
            boolean readOnly
    ) {
        var serializer = new BinaryTupleRowSerializer(storage.indexDescriptor());

        try (Cursor<IndexRow> cursor = readOnly
                ? storage.readOnlyScan(lowerBound, upperBound, flags)
                : storage.scan(lowerBound, upperBound, flags)
        ) {
            return cursor.stream()
                    .map(serializer::deserializeColumns)
                    .map(mapper)
                    .collect(toUnmodifiableList());
        }
    }

    private static List<Object[]> scan(SortedIndexStorage index, Function<SortedIndexStorage, Cursor<IndexRow>> cursorFunction) {
        var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

        try (Cursor<IndexRow> cursor = cursorFunction.apply(index)) {
            return cursor.stream()
                    .map(serializer::deserializeColumns)
                    .collect(toUnmodifiableList());
        }
    }

    private static Cursor<IndexRow> getIndexRowCursor(
            SortedIndexStorage indexStorage,
            BinaryTuplePrefix lowerBound,
            BinaryTuplePrefix upperBound,
            int flags,
            boolean readOnly
    ) {
        return readOnly
                ? indexStorage.readOnlyScan(lowerBound, upperBound, flags)
                : indexStorage.scan(lowerBound, upperBound, flags);
    }

    private static Stream<Arguments> allTypesColumnParamsAndReadOnly() {
        return Stream.concat(
                ALL_TYPES_COLUMN_PARAMS.stream()
                        .map(param -> Arguments.of(param, false)),
                ALL_TYPES_COLUMN_PARAMS.stream()
                        .map(param -> Arguments.of(param, true))
        );
    }

    private static <T> Function<IndexRow, T> firstColumn(BinaryTupleRowSerializer serializer) {
        return indexRow -> (T) serializer.deserializeColumns(indexRow)[0];
    }

    private static <T> T firstArrayElement(Object[] objects) {
        return (T) objects[0];
    }

    private static final class SimpleRow<T> {
        private final T indexColumns;

        private final RowId rowId;

        private SimpleRow(T indexColumns, RowId rowId) {
            this.indexColumns = indexColumns;
            this.rowId = rowId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SimpleRow<?> simpleRow = (SimpleRow<?>) o;
            return Objects.equals(indexColumns, simpleRow.indexColumns) && Objects.equals(rowId, simpleRow.rowId);
        }

        private static <T> SimpleRow<T> of(T indexColumns, RowId rowId) {
            return new SimpleRow<>(indexColumns, rowId);
        }

        private static <T> SimpleRow<T> of(IndexRow indexRow, Function<IndexRow, T> mapper) {
            return new SimpleRow<>(mapper.apply(indexRow), indexRow.rowId());
        }
    }

    private static CatalogIndexColumnDescriptor[] toCatalogIndexColumnDescriptors(ColumnType... columnTypes) {
        return Stream.of(columnTypes)
                .map(AbstractIndexStorageTest::columnName)
                .map(columnName -> new CatalogIndexColumnDescriptor(columnName, ASC_NULLS_FIRST))
                .toArray(CatalogIndexColumnDescriptor[]::new);
    }
}
