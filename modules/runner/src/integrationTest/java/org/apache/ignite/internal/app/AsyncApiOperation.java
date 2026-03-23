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
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.internal.streamer.SimplePublisher;
import org.apache.ignite.internal.table.partition.HashPartition;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;

/**
 * Asynchronous API operation.
 */
enum AsyncApiOperation {
    TABLES_TABLES(refs -> refs.tables.tablesAsync()),
    TABLES_TABLE(refs -> refs.tables.tableAsync(TEST_TABLE_NAME)),

    KV_VIEW_GET(refs -> refs.kvView.getAsync(null, KEY_TUPLE)),
    KV_VIEW_GET_NULLABLE(refs -> refs.kvView.getNullableAsync(null, KEY_TUPLE)),
    KV_VIEW_GET_OR_DEFAULT(refs -> refs.kvView.getOrDefaultAsync(null, KEY_TUPLE, null)),
    KV_VIEW_GET_ALL(refs -> refs.kvView.getAllAsync(null, List.of(KEY_TUPLE))),
    KV_VIEW_CONTAINS(refs -> refs.kvView.containsAsync(null, KEY_TUPLE)),
    KV_VIEW_CONTAINS_ALL(refs -> refs.kvView.containsAllAsync(null, List.of(KEY_TUPLE))),
    KV_VIEW_PUT(refs -> refs.kvView.putAsync(null, KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_PUT_ALL(refs -> refs.kvView.putAllAsync(null, Map.of(KEY_TUPLE, VALUE_TUPLE))),
    KV_VIEW_GET_AND_PUT(refs -> refs.kvView.getAndPutAsync(null, KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_GET_NULLABLE_AND_PUT(refs -> refs.kvView.getNullableAndPutAsync(null, KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_PUT_IF_ABSENT(refs -> refs.kvView.putIfAbsentAsync(null, KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_REMOVE(refs -> refs.kvView.removeAsync(KEY_TUPLE)),
    KV_VIEW_REMOVE_EXACT(refs -> refs.kvView.removeAsync(null, KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_REMOVE_ALL(refs -> refs.kvView.removeAllAsync(null, List.of(KEY_TUPLE))),
    KV_VIEW_GET_AND_REMOVE(refs -> refs.kvView.getAndRemoveAsync(null, KEY_TUPLE)),
    KV_VIEW_GET_NULLABLE_AND_REMOVE(refs -> refs.kvView.getNullableAndRemoveAsync(null, KEY_TUPLE)),
    KV_VIEW_REPLACE(refs -> refs.kvView.replaceAsync(KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_REPLACE_EXACT(refs -> refs.kvView.replaceAsync(null, KEY_TUPLE, VALUE_TUPLE, VALUE_TUPLE)),
    KV_VIEW_GET_AND_REPLACE(refs -> refs.kvView.getAndReplaceAsync(null, KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_GET_NULLABLE_AND_REPLACE(refs -> refs.kvView.getNullableAndReplaceAsync(null, KEY_TUPLE, VALUE_TUPLE)),
    KV_VIEW_STREAM_DATA(refs -> {
        CompletableFuture<?> future;
        try (var publisher = new SimplePublisher<Entry<Tuple, Tuple>>()) {
            future = refs.kvView.streamData(publisher, null);
            publisher.submit(Map.entry(KEY_TUPLE, VALUE_TUPLE));
        }
        return future;
    }),
    KV_VIEW_QUERY(refs -> refs.kvView.queryAsync(null, null)),
    KV_VIEW_QUERY_WITH_INDEX(refs -> refs.kvView.queryAsync(null, null, null)),
    KV_VIEW_QUERY_WITH_OPTIONS(refs -> refs.kvView.queryAsync(null, null, null, null)),

    TYPED_KV_VIEW_GET(refs -> refs.typedKvView.getAsync(null, 1)),
    MAPPED_KV_VIEW_GET(refs -> refs.mappedKvView.getAsync(null, 1)),

    RECORD_VIEW_GET(refs -> refs.recordView.getAsync(null, KEY_TUPLE)),
    RECORD_VIEW_GET_ALL(refs -> refs.recordView.getAllAsync(null, List.of(KEY_TUPLE))),
    RECORD_VIEW_CONTAINS(refs -> refs.recordView.containsAsync(null, KEY_TUPLE)),
    RECORD_VIEW_CONTAINS_ALL(refs -> refs.recordView.containsAllAsync(null, List.of(KEY_TUPLE))),
    RECORD_VIEW_UPSERT(refs -> refs.recordView.upsertAsync(null, FULL_TUPLE)),
    RECORD_VIEW_UPSERT_ALL(refs -> refs.recordView.upsertAllAsync(null, List.of(FULL_TUPLE))),
    RECORD_VIEW_GET_AND_UPSERT(refs -> refs.recordView.getAndUpsertAsync(null, FULL_TUPLE)),
    RECORD_VIEW_INSERT(refs -> refs.recordView.insertAsync(null, FULL_TUPLE)),
    RECORD_VIEW_INSERT_ALL(refs -> refs.recordView.insertAllAsync(null, List.of(FULL_TUPLE))),
    RECORD_VIEW_REPLACE(refs -> refs.recordView.replaceAsync(null, FULL_TUPLE)),
    RECORD_VIEW_REPLACE_EXACT(refs -> refs.recordView.replaceAsync(null, FULL_TUPLE, FULL_TUPLE)),
    RECORD_VIEW_GET_AND_REPLACE(refs -> refs.recordView.getAndReplaceAsync(null, FULL_TUPLE)),
    RECORD_VIEW_DELETE(refs -> refs.recordView.deleteAsync(null, KEY_TUPLE)),
    RECORD_VIEW_DELETE_EXACT(refs -> refs.recordView.deleteExactAsync(null, FULL_TUPLE)),
    RECORD_VIEW_GET_AND_DELETE(refs -> refs.recordView.getAndDeleteAsync(null, KEY_TUPLE)),
    RECORD_VIEW_DELETE_ALL(refs -> refs.recordView.deleteAllAsync(null, List.of(KEY_TUPLE))),
    RECORD_VIEW_DELETE_ALL_EXACT(refs -> refs.recordView.deleteAllExactAsync(null, List.of(FULL_TUPLE))),
    RECORD_VIEW_STREAM_DATA(refs -> {
        CompletableFuture<?> future;
        try (var publisher = new SimplePublisher<Tuple>()) {
            future = refs.recordView.streamData(publisher, null);
            publisher.submit(FULL_TUPLE);
        }
        return future;
    }),
    RECORD_VIEW_QUERY(refs -> refs.recordView.queryAsync(null, null)),
    RECORD_VIEW_QUERY_WITH_INDEX(refs -> refs.recordView.queryAsync(null, null, null)),
    RECORD_VIEW_QUERY_WITH_OPTIONS(refs -> refs.recordView.queryAsync(null, null, null, null)),

    TYPED_RECORD_VIEW_GET(refs -> refs.typedRecordView.getAsync(null, new Record(1, ""))),

    MAPPED_RECORD_VIEW_GET(refs -> refs.mappedRecordView.getAsync(null, new Record(1, ""))),

    PARTITION_MANAGER_PARTITIONS(refs -> refs.partitionManager.partitionsAsync()),
    PARTITION_MANAGER_PRIMARY_REPLICA(refs -> refs.partitionManager.primaryReplicaAsync(new HashPartition(0))),
    PARTITION_MANAGER_PRIMARY_REPLICAS(refs -> refs.partitionManager.primaryReplicasAsync()),
    PARTITION_MANAGER_PRIMARY_REPLICAS_BY_NODE(refs -> refs.partitionManager.primaryReplicasAsync(refs.clusterNodes.iterator().next())),
    PARTITION_MANAGER_PARTITION_BY_KEY(refs -> refs.partitionManager.partitionAsync(1, Mapper.of(Integer.class))),
    PARTITION_MANAGER_PARTITION_BY_TUPLE(refs -> refs.partitionManager.partitionAsync(KEY_TUPLE)),

    PARTITION_DISTRIBUTION_PARTITIONS(refs -> refs.partitionDistribution.partitionsAsync()),
    PARTITION_DISTRIBUTION_PRIMARY_REPLICA(refs -> refs.partitionDistribution.primaryReplicaAsync(new HashPartition(0))),
    PARTITION_DISTRIBUTION_PRIMARY_REPLICAS(refs -> refs.partitionDistribution.primaryReplicasAsync()),
    PARTITION_DISTRIBUTION_PRIMARY_REPLICAS_BY_NODE(refs ->
            refs.partitionManager.primaryReplicasAsync(refs.clusterNodes.iterator().next())),
    PARTITION_DISTRIBUTION_PARTITION_BY_KEY(refs -> refs.partitionDistribution.partitionAsync(1, Mapper.of(Integer.class))),
    PARTITION_DISTRIBUTION_PARTITION_BY_TUPLE(refs -> refs.partitionDistribution.partitionAsync(KEY_TUPLE)),

    TRANSACTIONS_BEGIN(refs -> refs.transactions.beginAsync()),
    TRANSACTIONS_BEGIN_WITH_OPTS(refs -> refs.transactions.beginAsync(null)),
    TRANSACTIONS_RUN_IN_TRANSACTION(refs -> refs.transactions.runInTransactionAsync(tx -> nullCompletedFuture())),
    TRANSACTIONS_RUN_IN_TRANSACTION_WITH_OPTS(refs -> refs.transactions.runInTransactionAsync(tx -> nullCompletedFuture(), null)),

    SQL_EXECUTE(refs -> refs.sql.executeAsync(SELECT_IDS_QUERY)),
    SQL_EXECUTE_STATEMENT(refs -> refs.sql.executeAsync((Transaction) null, refs.selectIdsStatement)),
    // TODO: IGNITE-18695 - uncomment the following 2 lines.
    // SQL_EXECUTE_WITH_MAPPER(refs -> refs.sql.executeAsync(null, Mapper.of(Integer.class), SELECT_IDS_QUERY)),
    // SQL_EXECUTE_STATEMENT_WITH_MAPPER(refs -> refs.sql.executeAsync(null, Mapper.of(Integer.class), refs.selectIdsStatement)),
    SQL_EXECUTE_BATCH(refs -> refs.sql.executeBatchAsync(null, UPDATE_QUERY, BatchedArguments.of(999))),
    SQL_EXECUTE_BATCH_STATEMENT(refs -> refs.sql.executeBatchAsync(null, refs.updateStatement, BatchedArguments.of(999))),
    SQL_EXECUTE_SCRIPT(refs -> refs.sql.executeScriptAsync(SELECT_IDS_QUERY)),

    COMPUTE_SUBMIT(refs -> refs.compute.submitAsync(
            anyNode(refs.clusterNodes), JobDescriptor.builder(NoOpJob.class).build(), null
    )),
    COMPUTE_SUBMIT_BROADCAST(refs -> refs.compute.submitAsync(
            BroadcastJobTarget.nodes(refs.clusterNodes),
            JobDescriptor.builder(NoOpJob.class).build(),
            null
    )),
    COMPUTE_EXECUTE(refs -> refs.compute.executeAsync(
            anyNode(refs.clusterNodes), JobDescriptor.builder(NoOpJob.class).build(), null
    )),
    COMPUTE_EXECUTE_BROADCAST(refs -> refs.compute.executeAsync(
            BroadcastJobTarget.nodes(refs.clusterNodes),
            JobDescriptor.builder(NoOpJob.class).build(),
            null
    )),
    COMPUTE_EXECUTE_MAP_REDUCE(refs -> refs.compute.executeMapReduceAsync(TaskDescriptor.builder(NoOpMapReduceTask.class).build(), null)),

    CATALOG_CREATE_TABLE_BY_RECORD_CLASS(refs -> refs.catalog.createTableAsync(Pojo.class)),
    CATALOG_CREATE_TABLE_BY_KEY_VALUE_CLASSES(refs -> refs.catalog.createTableAsync(PojoKey.class, PojoValue.class)),
    CATALOG_CREATE_TABLE_BY_DEFINITION(refs -> refs.catalog.createTableAsync(tableDefinition())),

    CATALOG_CREATE_ZONE(refs -> refs.catalog.createZoneAsync(zoneDefinition())),

    CATALOG_DROP_TABLE_BY_NAME(refs -> refs.catalog.dropTableAsync(tableDefinition().tableName())),
    CATALOG_DROP_TABLE_BY_DEFINITION(refs -> refs.catalog.dropTableAsync(tableDefinition())),

    CATALOG_DROP_ZONE_BY_NAME(refs -> refs.catalog.dropZoneAsync(zoneDefinition().zoneName())),
    CATALOG_DROP_ZONE_BY_DEFINITION(refs -> refs.catalog.dropZoneAsync(zoneDefinition()));

    private final Function<References, CompletableFuture<?>> action;

    AsyncApiOperation(Function<References, CompletableFuture<?>> action) {
        this.action = action;
    }

    CompletableFuture<?> execute(References references) {
        return action.apply(references);
    }
}
