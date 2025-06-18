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

package org.apache.ignite.internal.app;

import static org.apache.ignite.compute.JobTarget.anyNode;
import static org.apache.ignite.internal.app.ApiReferencesTestUtils.FULL_TUPLE;
import static org.apache.ignite.internal.app.ApiReferencesTestUtils.KEY_TUPLE;
import static org.apache.ignite.internal.app.ApiReferencesTestUtils.SELECT_IDS_QUERY;
import static org.apache.ignite.internal.app.ApiReferencesTestUtils.TEST_TABLE_NAME;
import static org.apache.ignite.internal.app.ApiReferencesTestUtils.UPDATE_QUERY;
import static org.apache.ignite.internal.app.ApiReferencesTestUtils.VALUE_TUPLE;
import static org.apache.ignite.internal.app.ApiReferencesTestUtils.tableDefinition;
import static org.apache.ignite.internal.app.ApiReferencesTestUtils.zoneDefinition;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.table.mapper.Mapper;

/**
 * Synchronous API operation.
 */
@SuppressWarnings("resource")
enum SyncApiOperation {
    IGNITE_NAME(refs -> refs.ignite.name()),
    IGNITE_TABLES(refs -> refs.ignite.tables()),
    IGNITE_TRANSACTIONS(refs -> refs.ignite.transactions()),
    IGNITE_SQL(refs -> refs.ignite.sql()),
    IGNITE_COMPUTE(refs -> refs.ignite.compute()),
    IGNITE_CLUSTER_NODES(refs -> refs.ignite.cluster().nodes()),
    IGNITE_CATALOG(refs -> refs.ignite.catalog()),

    TABLES_TABLES(refs -> refs.tables.tables()),
    TABLES_TABLE(refs -> refs.tables.table(TEST_TABLE_NAME)),

    TABLE_NAME(refs -> refs.table.name()),
    TABLE_QUALIFIED_NAME(refs -> refs.table.qualifiedName()),
    TABLE_KV_VIEW(refs -> refs.table.keyValueView()),
    TABLE_TYPED_KV_VIEW(refs -> refs.table.keyValueView(Integer.class, String.class)),
    TABLE_MAPPED_KV_VIEW(refs -> refs.table.keyValueView(Mapper.of(Integer.class), Mapper.of(String.class))),
    TABLE_RECORD_VIEW(refs -> refs.table.recordView()),
    TABLE_TYPED_RECORD_VIEW(refs -> refs.table.recordView(Record.class)),
    TABLE_MAPPED_RECORD_VIEW(refs -> refs.table.recordView(Mapper.of(Record.class))),
    TABLE_PARTITION_MANAGER(refs -> refs.table.partitionManager()),

    TABLE_FROM_TABLE_ASYNC_PUT(refs -> refs.tableFromTableAsync.keyValueView().put(null, KEY_TUPLE, VALUE_TUPLE)),
    TABLE_FROM_TABLES_PUT(refs -> refs.tableFromTables.keyValueView().put(null, KEY_TUPLE, VALUE_TUPLE)),
    TABLE_FROM_TABLES_ASYNC_PUT(refs -> refs.tableFromTablesAsync.keyValueView().put(null, KEY_TUPLE, VALUE_TUPLE)),

    KV_VIEW_GET(refs -> refs.kvView.get(null, KEY_TUPLE)),
    KV_VIEW_GET_NULLABLE(refs -> refs.kvView.getNullable(null, KEY_TUPLE)),
    KV_VIEW_GET_OR_DEFAULT(refs -> refs.kvView.getOrDefault(null, KEY_TUPLE, null)),
    KV_VIEW_GET_ALL(refs -> refs.kvView.getAll(null, List.of(KEY_TUPLE))),
    KV_VIEW_CONTAINS(refs -> refs.kvView.contains(null, KEY_TUPLE)),
    KV_VIEW_CONTAINS_ALL(refs -> refs.kvView.containsAll(null, List.of(KEY_TUPLE))),
    KV_VIEW_PUT(refs -> refs.kvView.put(null, KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_PUT_ALL(refs -> refs.kvView.putAll(null, Map.of(KEY_TUPLE, VALUE_TUPLE))),
    KV_VIEW_GET_AND_PUT(refs -> refs.kvView.getAndPut(null, KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_GET_NULLABLE_AND_PUT(refs -> refs.kvView.getNullableAndPut(null, KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_PUT_IF_ABSENT(refs -> refs.kvView.putIfAbsent(null, KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_REMOVE(refs -> refs.kvView.remove(null, KEY_TUPLE)),
    KV_VIEW_REMOVE_EXACT(refs -> refs.kvView.remove(null, KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_REMOVE_ALL(refs -> refs.kvView.removeAll(null, List.of(KEY_TUPLE))),
    KV_VIEW_GET_AND_REMOVE(refs -> refs.kvView.getAndRemove(null, KEY_TUPLE)),
    KV_VIEW_GET_NULLABLE_AND_REMOVE(refs -> refs.kvView.getNullableAndRemove(null, KEY_TUPLE)),
    KV_VIEW_REPLACE(refs -> refs.kvView.replace(null, KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_REPLACE_EXACT(refs -> refs.kvView.replace(null, KEY_TUPLE, VALUE_TUPLE, VALUE_TUPLE)),
    KV_VIEW_GET_AND_REPLACE(refs -> refs.kvView.getAndReplace(null, KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_GET_NULLABLE_AND_REPLACE(refs -> refs.kvView.getNullableAndReplace(null, KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_QUERY(refs -> refs.kvView.query(null, null)),
    KV_VIEW_QUERY_WITH_INDEX(refs -> refs.kvView.query(null, null, null)),
    KV_VIEW_QUERY_WITH_OPTIONS(refs -> refs.kvView.query(null, null, null, null)),

    TYPED_KV_VIEW_GET(refs -> refs.typedKvView.get(null, 1)),
    MAPPED_KV_VIEW_GET(refs -> refs.mappedKvView.get(null, 1)),

    RECORD_VIEW_GET(refs -> refs.recordView.get(null, KEY_TUPLE)),
    RECORD_VIEW_GET_ALL(refs -> refs.recordView.getAll(null, List.of(KEY_TUPLE))),
    RECORD_VIEW_CONTAINS(refs -> refs.recordView.contains(null, KEY_TUPLE)),
    RECORD_VIEW_CONTAINS_ALL(refs -> refs.recordView.containsAll(null, List.of(KEY_TUPLE))),
    RECORD_VIEW_UPSERT(refs -> refs.recordView.upsert(null, FULL_TUPLE)),
    RECORD_VIEW_UPSERT_ALL(refs -> refs.recordView.upsertAll(null, List.of(FULL_TUPLE))),
    RECORD_VIEW_GET_AND_UPSERT(refs -> refs.recordView.getAndUpsert(null, FULL_TUPLE)),
    RECORD_VIEW_INSERT(refs -> refs.recordView.insert(null, FULL_TUPLE)),
    RECORD_VIEW_INSERT_ALL(refs -> refs.recordView.insertAll(null, List.of(FULL_TUPLE))),
    RECORD_VIEW_REPLACE(refs -> refs.recordView.replace(null, FULL_TUPLE)),
    RECORD_VIEW_REPLACE_EXACT(refs -> refs.recordView.replace(null, FULL_TUPLE, FULL_TUPLE)),
    RECORD_VIEW_GET_AND_REPLACE(refs -> refs.recordView.getAndReplace(null, FULL_TUPLE)),
    RECORD_VIEW_DELETE(refs -> refs.recordView.delete(null, KEY_TUPLE)),
    RECORD_VIEW_DELETE_EXACT(refs -> refs.recordView.deleteExact(null, FULL_TUPLE)),
    RECORD_VIEW_GET_AND_DELETE(refs -> refs.recordView.getAndDelete(null, KEY_TUPLE)),
    RECORD_VIEW_DELETE_ALL(refs -> refs.recordView.deleteAll(null, List.of(KEY_TUPLE))),
    RECORD_VIEW_DELETE_ALL_EXACT(refs -> refs.recordView.deleteAllExact(null, List.of(FULL_TUPLE))),

    TYPED_RECORD_VIEW_GET(refs -> refs.typedRecordView.get(null, new Record(1, ""))),

    MAPPED_RECORD_VIEW_GET(refs -> refs.mappedRecordView.get(null, new Record(1, ""))),

    TRANSACTIONS_BEGIN(refs -> refs.transactions.begin()),
    TRANSACTIONS_BEGIN_WITH_OPTS(refs -> refs.transactions.begin(null)),
    TRANSACTIONS_RUN_CONSUMER_IN_TRANSACTION(refs -> refs.transactions.runInTransaction(tx -> {})),
    TRANSACTIONS_RUN_CONSUMER_IN_TRANSACTION_WITH_OPTS(refs -> refs.transactions.runInTransaction(tx -> {}, null)),
    TRANSACTIONS_RUN_FUNCTION_IN_TRANSACTION(refs -> refs.transactions.runInTransaction(tx -> null)),
    TRANSACTIONS_RUN_FUNCTION_IN_TRANSACTION_WITH_OPTS(refs -> refs.transactions.runInTransaction(tx -> null, null)),

    SQL_CREATE_STATEMENT(refs -> refs.sql.createStatement(SELECT_IDS_QUERY)),
    SQL_STATEMENT_BUILDER(refs -> refs.sql.statementBuilder()),
    SQL_EXECUTE(refs -> refs.sql.execute(null, SELECT_IDS_QUERY)),
    SQL_EXECUTE_STATEMENT(refs -> refs.sql.execute(null, refs.selectIdsStatement)),
    // TODO: IGNITE-18695 - uncomment the following 2 lines.
    // SQL_EXECUTE_WITH_MAPPER(refs -> refs.sql.execute(null, Mapper.of(Integer.class), SELECT_IDS_QUERY)),
    // SQL_EXECUTE_STATEMENT_WITH_MAPPER(refs -> refs.sql.execute(null, Mapper.of(Integer.class), refs.selectIdsStatement)),
    SQL_EXECUTE_BATCH(refs -> refs.sql.executeBatch(null, UPDATE_QUERY, BatchedArguments.of(999))),
    SQL_EXECUTE_BATCH_STATEMENT(refs -> refs.sql.executeBatch(null, refs.updateStatement, BatchedArguments.of(999))),
    SQL_EXECUTE_SCRIPT(refs -> refs.sql.executeScript(SELECT_IDS_QUERY)),

    COMPUTE_EXECUTE(refs -> refs.compute.execute(anyNode(refs.clusterNodes), JobDescriptor.builder(NoOpJob.class).build(), null)),
    COMPUTE_EXECUTE_BROADCAST(refs -> refs.compute.execute(
            BroadcastJobTarget.nodes(refs.clusterNodes),
            JobDescriptor.builder(NoOpJob.class).build(),
            null
    )),
    COMPUTE_SUBMIT_MAP_REDUCE(refs -> refs.compute.submitMapReduce(
            TaskDescriptor.builder(NoOpMapReduceTask.class).build(),
            null
    )),
    COMPUTE_EXECUTE_MAP_REDUCE(refs -> refs.compute.executeMapReduce(
            TaskDescriptor.builder(NoOpMapReduceTask.class).build(),
            null
    )),

    CATALOG_CREATE_TABLE_BY_RECORD_CLASS(refs -> refs.catalog.createTable(Pojo.class)),
    CATALOG_CREATE_TABLE_BY_KEY_VALUE_CLASSES(refs -> refs.catalog.createTable(PojoKey.class, PojoValue.class)),
    CATALOG_CREATE_TABLE_BY_DEFINITION(refs -> refs.catalog.createTable(tableDefinition())),

    CATALOG_CREATE_ZONE(refs -> refs.catalog.createZone(zoneDefinition())),

    CATALOG_DROP_TABLE_BY_NAME(refs -> refs.catalog.dropTable(tableDefinition().tableName())),
    CATALOG_DROP_TABLE_BY_DEFINITION(refs -> refs.catalog.dropTable(tableDefinition())),

    CATALOG_DROP_ZONE_BY_NAME(refs -> refs.catalog.dropZone(zoneDefinition().zoneName())),
    CATALOG_DROP_ZONE_BY_DEFINITION(refs -> refs.catalog.dropZone(zoneDefinition()));

    private final Consumer<References> action;

    SyncApiOperation(Consumer<References> action) {
        this.action = action;
    }

    void execute(References references) {
        action.accept(references);
    }

    boolean worksAfterShutdown() {
        return this == IGNITE_TABLES
                || this == IGNITE_TRANSACTIONS
                || this == IGNITE_SQL
                || this == IGNITE_COMPUTE
                || this == IGNITE_CATALOG;
    }
}
