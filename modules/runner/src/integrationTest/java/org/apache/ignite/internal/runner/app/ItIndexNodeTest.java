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

package org.apache.ignite.internal.runner.app;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.GREATER_OR_EQUAL;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.LESS_OR_EQUAL;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.flow.TestFlowUtils.subscribeToList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.table.IndexScanCriteria;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.OperationContext;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.TxContext;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests running against a single Ignite node validating index behavior.
 */
public class ItIndexNodeTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "IT_INDEX_TEST_TABLE";
    private static final String HASH_IDX = "TEST_HASH_IDX";
    private static final String SORT_IDX = "TEST_SORT_IDX";
    private static final int LOWER_BOUND = 10;
    private static final int UPPER_BOUND = 20;
    private static final int VALUE = 15;
    private static final int PART_ID = 0;
    private static final List<Integer> ALL_VALUES_IN_RANGE = List.of(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

    private InternalTable internalTable;
    private int hashIdx;
    private int sortIdx;

    TableViewInternal tableViewInternal;

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeAll
    void setUp(TestInfo testInfo) {
        Ignite ignite = CLUSTER.node(0);

        // Create zone with 1 partition.
        String createZoneSql = "CREATE ZONE IF NOT EXISTS test_zone (PARTITIONS 1, REPLICAS 1) STORAGE PROFILES ['default']";

        String createTableSql = "CREATE TABLE " + TABLE_NAME + "(\n"
                + "    key int PRIMARY KEY,\n"
                + "    field1   int,\n"
                + "    field2   int\n"
                + ") ZONE test_zone";
        String createHashIndexSql = "CREATE INDEX IF NOT EXISTS " + HASH_IDX + " ON " + TABLE_NAME + " USING HASH (field1)";
        String createSortIndexSql = "CREATE INDEX IF NOT EXISTS " + SORT_IDX + " ON " + TABLE_NAME + " USING SORTED (field2)";
        String insertWithParams = "INSERT INTO " + TABLE_NAME + " (key, field1, field2) VALUES (?, ?, ?)";

        ignite.sql().execute(createZoneSql);
        ignite.sql().execute(createTableSql);
        ignite.sql().execute(createHashIndexSql);
        ignite.sql().execute(createSortIndexSql);
        for (int i = LOWER_BOUND; i <= UPPER_BOUND; i++) {
            ignite.sql().execute(insertWithParams, i, i, i);
        }

        IgniteImpl igniteImpl = unwrapIgniteImpl(ignite);

        CatalogManager catalogManager = igniteImpl.catalogManager();

        Catalog catalog = catalogManager.activeCatalog(igniteImpl.clock().nowLong());

        hashIdx = getIndexIdBy(HASH_IDX, catalog);
        sortIdx = getIndexIdBy(SORT_IDX, catalog);

        // Get the public Table API.
        Table table = ignite.tables().table(TABLE_NAME);

        assertNotNull(table);

        // Unwrap to get TableViewInternal.
        tableViewInternal = unwrapTableViewInternal(table);

        // Get the InternalTable.
        internalTable = tableViewInternal.internalTable();

        assertNotNull(internalTable);
    }

    @Test
    void testHashIndexThrowsWithRangeScan() {
        Publisher<BinaryRow> publisher =  internalTable.scan(PART_ID, null, hashIdx,
                createMatchingRange());
        CompletableFuture<List<BinaryRow>> resultFuture = subscribeToList(publisher);
        assertThrowsWithCause(() -> {
            resultFuture.get(10, TimeUnit.SECONDS);
        }, IllegalStateException.class, "Scan works only with sorted index.");
    }

    @Test
    void testReadOnlyHashIndexThrowsWithRangeScan() {
        Ignite ignite = CLUSTER.node(0);
        IgniteImpl igniteImpl = unwrapIgniteImpl(ignite);
        InternalTransaction tx = (InternalTransaction) ignite.transactions().begin(new TransactionOptions().readOnly(true));
        OperationContext operationContext = OperationContext.create(TxContext.readOnly(tx));
        Publisher<BinaryRow> publisher = internalTable.scan(PART_ID, igniteImpl.node(), hashIdx,
                createMatchingRange(), operationContext);
        CompletableFuture<List<BinaryRow>> resultFuture = subscribeToList(publisher);
        assertThrowsWithCause(() -> {
            resultFuture.get(10, TimeUnit.SECONDS);
        }, IllegalStateException.class, "Scan works only with sorted index.");
    }

    @Test
    void testHashIndexThrowsWithUnboundRangeScan() {
        Publisher<BinaryRow> publisher =  internalTable.scan(PART_ID, null, hashIdx,
                IndexScanCriteria.unbounded());
        CompletableFuture<List<BinaryRow>> resultFuture = subscribeToList(publisher);
        assertThrowsWithCause(() -> {
            resultFuture.get(10, TimeUnit.SECONDS);
        }, IllegalStateException.class, "Scan works only with sorted index.");
    }

    @Test
    void testSortWorksWithRangeScan() {
        Publisher<BinaryRow> publisher =  internalTable.scan(PART_ID, null, sortIdx,
                createMatchingRange());
        CompletableFuture<List<BinaryRow>> resultFuture = subscribeToList(publisher);
        assertThat(resultFuture, willCompleteSuccessfully());
    }

    @Test
    void testSortFindsResultWithUnboundRangeScan() throws Exception {
        Publisher<BinaryRow> publisher =  internalTable.scan(PART_ID, null, sortIdx,
                IndexScanCriteria.unbounded());
        CompletableFuture<List<BinaryRow>> resultFuture = subscribeToList(publisher);
        assertThat(resultFuture, willCompleteSuccessfully());
        List<BinaryRow> result = resultFuture.get(10, TimeUnit.SECONDS);
        assertToBeCorrect(ALL_VALUES_IN_RANGE, result);
    }

    @Test
    void testSortFindsResultWithBoundRangeScan() throws Exception {
        Publisher<BinaryRow> publisher =  internalTable.scan(PART_ID, null, sortIdx,
                createMatchingRange());
        CompletableFuture<List<BinaryRow>> resultFuture = subscribeToList(publisher);
        assertThat(resultFuture, willCompleteSuccessfully());
        List<BinaryRow> result = resultFuture.get(10, TimeUnit.SECONDS);
        assertToBeCorrect(ALL_VALUES_IN_RANGE, result);
    }

    @Test
    void testSortFindsNothingWithUnmatchingRangeScan() throws Exception {
        Publisher<BinaryRow> publisher =  internalTable.scan(PART_ID, null, sortIdx,
                createUnmatchingRange());
        CompletableFuture<List<BinaryRow>> resultFuture = subscribeToList(publisher);
        assertThat(resultFuture, willCompleteSuccessfully());
        List<BinaryRow> result = resultFuture.get(10, TimeUnit.SECONDS);
        assertThat(result, hasSize(0));
    }

    @Test
    void testSortFindsNothingWithUnmatchingRangeComplementScan() throws Exception {
        Publisher<BinaryRow> publisher =  internalTable.scan(PART_ID, null, sortIdx,
                createUnmatchingRangeComplement());
        CompletableFuture<List<BinaryRow>> resultFuture = subscribeToList(publisher);
        assertThat(resultFuture, willCompleteSuccessfully());
        List<BinaryRow> result = resultFuture.get(10, TimeUnit.SECONDS);
        assertThat(result, hasSize(0));
    }

    @Test
    void testScanFailsWithNonExistentIndex() {
        // Use a non-existent index ID.
        int nonExistentIndexId = -1;
        Publisher<BinaryRow> publisher = internalTable.scan(PART_ID, null, nonExistentIndexId, IndexScanCriteria.unbounded());
        CompletableFuture<List<BinaryRow>> resultFuture = subscribeToList(publisher);
        assertThrowsWithCause(() -> {
            resultFuture.get(10, TimeUnit.SECONDS);
        }, IllegalStateException.class, "Index not found: [id=-1].");
    }

    @Test
    void testReadOnlyScanFailsWithNonExistentIndex() {
        Ignite ignite = CLUSTER.node(0);
        IgniteImpl igniteImpl = unwrapIgniteImpl(ignite);
        // Use a non-existent index ID.
        int nonExistentIndexId = -1;
        InternalTransaction tx = (InternalTransaction) ignite.transactions().begin(new TransactionOptions().readOnly(true));
        OperationContext operationContext = OperationContext.create(TxContext.readOnly(tx));
        Publisher<BinaryRow> publisher = internalTable.scan(PART_ID, igniteImpl.node(), nonExistentIndexId,
                IndexScanCriteria.unbounded(), operationContext);
        CompletableFuture<List<BinaryRow>> resultFuture = subscribeToList(publisher);
        assertThrowsWithCause(() -> {
            resultFuture.get(10, TimeUnit.SECONDS);
        }, IllegalStateException.class, "Index not found: [id=-1].");
    }

    @Test
    void testHashIndexLookupExactMatch() throws Exception {
        Ignite ignite = CLUSTER.node(0);
        // Create a BinaryTuple with value 15 for the hash index lookup (field1 = 15)
        BinaryTuple lookupKey = new BinaryTuple(1, new BinaryTupleBuilder(1).appendInt(VALUE).build());
        IndexScanCriteria.Lookup lookupCriteria = IndexScanCriteria.lookup(lookupKey);

        InternalTransaction tx = (InternalTransaction) ignite.transactions().begin(new TransactionOptions().readOnly(true));
        OperationContext operationContext = OperationContext.create(TxContext.readOnly(tx));

        Publisher<BinaryRow> publisher = internalTable.scan(PART_ID, unwrapIgniteImpl(ignite).node(), hashIdx,
                lookupCriteria, operationContext);
        CompletableFuture<List<BinaryRow>> resultFuture = subscribeToList(publisher);

        assertThat(resultFuture, willCompleteSuccessfully());
        List<BinaryRow> result = resultFuture.get(10, TimeUnit.SECONDS);

        assertThat(result, hasSize(1));
        assertToBeCorrect(List.of(VALUE), result);
    }

    private int getIndexIdBy(String name, Catalog catalog) {
        String schemaName = SqlCommon.DEFAULT_SCHEMA_NAME;
        CatalogIndexDescriptor indexDesc = catalog.aliveIndex(schemaName, name);
        int res;
        if (indexDesc != null) {
            res = indexDesc.id();
        } else {
            throw new IllegalStateException();
        }
        return res;
    }

    private void assertToBeCorrect(List<Integer> expected, List<BinaryRow> result) {
        SchemaDescriptor schema = tableViewInternal.schemaView().lastKnownSchema();
        List<Integer> actual = result.stream().map(row -> {
            BinaryTupleReader reader = new BinaryTupleReader(schema.length(), row.tupleSlice());
            // We always use key, since by test design all values are the same among key, field1, field2
            Column keyColumn = findColumnByName(schema, "key");
            return reader.intValue(keyColumn.positionInRow());
        }).collect(Collectors.toList());
        assertThat(actual, containsInAnyOrder(expected.toArray()));
    }

    private Column findColumnByName(SchemaDescriptor schema, String columnName) {
        return schema.columns().stream()
                .filter(column -> columnName.equalsIgnoreCase(column.name()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        String.format("Can't find column by name: [columnName=%s]", columnName)
                ));
    }

    private IndexScanCriteria.Range createMatchingRange() {
        return IndexScanCriteria.range(createBinaryTuplePrefix(LOWER_BOUND),
                createBinaryTuplePrefix(UPPER_BOUND), GREATER_OR_EQUAL | LESS_OR_EQUAL);
    }

    private IndexScanCriteria.Range createUnmatchingRange() {
        return IndexScanCriteria.range(createBinaryTuplePrefix(LOWER_BOUND * 3),
                createBinaryTuplePrefix(UPPER_BOUND * 3), GREATER_OR_EQUAL | LESS_OR_EQUAL);
    }

    private IndexScanCriteria.Range createUnmatchingRangeComplement() {
        return IndexScanCriteria.range(createBinaryTuplePrefix(UPPER_BOUND),
                createBinaryTuplePrefix(LOWER_BOUND), GREATER_OR_EQUAL | LESS_OR_EQUAL);
    }

    private BinaryTuplePrefix createBinaryTuplePrefix(int value) {
        BinaryTuple tuple = new BinaryTuple(1, new BinaryTupleBuilder(1).appendInt(value).build());
        return BinaryTuplePrefix.fromBinaryTuple(tuple);
    }
}
