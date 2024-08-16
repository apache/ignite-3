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

import static org.apache.ignite.internal.app.ApiReferencesTestUtils.FULL_TUPLE;
import static org.apache.ignite.internal.app.ApiReferencesTestUtils.KEY_TUPLE;
import static org.apache.ignite.internal.app.ApiReferencesTestUtils.TEST_TABLE_NAME;
import static org.apache.ignite.internal.app.ApiReferencesTestUtils.VALUE_TUPLE;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.streamer.SimplePublisher;
import org.apache.ignite.internal.table.partition.HashPartition;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

/**
 * Asynchronous API operation.
 */
enum AsyncApiOperation {
    TABLES_TABLES(refs -> refs.tables.tablesAsync()),
    TABLES_TABLE(refs -> refs.tables.tableAsync(TEST_TABLE_NAME)),

    KVVIEW_GET(refs -> refs.kvView.getAsync(null, KEY_TUPLE)),
    KVVIEW_GET_OR_DEFAULT(refs -> refs.kvView.getOrDefaultAsync(null, KEY_TUPLE, null)),
    KVVIEW_GET_ALL(refs -> refs.kvView.getAllAsync(null, List.of(KEY_TUPLE))),
    KVVIEW_CONTAINS(refs -> refs.kvView.containsAsync(null, KEY_TUPLE)),
    KVVIEW_CONTAINS_ALL(refs -> refs.kvView.containsAllAsync(null, List.of(KEY_TUPLE))),
    KVVIEW_PUT(refs -> refs.kvView.putAsync(null, KEY_TUPLE, VALUE_TUPLE)),
    KVVIEW_PUT_ALL(refs -> refs.kvView.putAllAsync(null, Map.of(KEY_TUPLE, VALUE_TUPLE))),
    KVVIEW_GET_AND_PUT(refs -> refs.kvView.getAndPutAsync(null, KEY_TUPLE, VALUE_TUPLE)),
    KVVIEW_PUT_IF_ABSENT(refs -> refs.kvView.putIfAbsentAsync(null, KEY_TUPLE, VALUE_TUPLE)),
    KVVIEW_REMOVE(refs -> refs.kvView.removeAsync(null, KEY_TUPLE)),
    KVVIEW_REMOVE_EXACT(refs -> refs.kvView.removeAsync(null, KEY_TUPLE, VALUE_TUPLE)),
    KVVIEW_REMOVE_ALL(refs -> refs.kvView.removeAllAsync(null, List.of(KEY_TUPLE))),
    KVVIEW_GET_AND_REMOVE(refs -> refs.kvView.getAndRemoveAsync(null, KEY_TUPLE)),
    KVVIEW_REPLACE(refs -> refs.kvView.replaceAsync(null, KEY_TUPLE, VALUE_TUPLE)),
    KVVIEW_REPLACE_EXACT(refs -> refs.kvView.replaceAsync(null, KEY_TUPLE, VALUE_TUPLE, VALUE_TUPLE)),
    KVVIEW_GET_AND_REPLACE(refs -> refs.kvView.getAndReplaceAsync(null, KEY_TUPLE, VALUE_TUPLE)),
    VKVIEW_STREAM_DATA(refs -> {
        CompletableFuture<?> future;
        try (var publisher = new SimplePublisher<Entry<Tuple, Tuple>>()) {
            future = refs.kvView.streamData(publisher, null);
            publisher.submit(Map.entry(KEY_TUPLE, VALUE_TUPLE));
        }
        return future;
    }),
    KVVIEW_QUERY(refs -> refs.kvView.queryAsync(null, null)),
    KVVIEW_QUERY_WITH_INDEX(refs -> refs.kvView.queryAsync(null, null, null)),
    KVVIEW_QUERY_WITH_OPTIONS(refs -> refs.kvView.queryAsync(null, null, null, null)),

    TYPED_KVVIEW_GET_NULLABLE(refs -> refs.typedKvView.getNullableAsync(null, 1)),
    TYPED_KVVIEW_GET_NULLABLE_AND_PUT(refs -> refs.typedKvView.getNullableAndPutAsync(null, 1, "one")),
    TYPED_KVVIEW_GET_NULLABLE_AND_REMOVE(refs -> refs.typedKvView.getNullableAndRemoveAsync(null, 1)),
    TYPED_KVVIEW_GET_NULLABLE_AND_REPLACE(refs -> refs.typedKvView.getNullableAndReplaceAsync(null, 1, "one")),

    MAPPED_KVVIEW_GET(refs -> refs.mappedKvView.getAsync(null, 1)),

    RECORDVIEW_GET(refs -> refs.recordView.getAsync(null, KEY_TUPLE)),
    RECORDVIEW_GET_ALL(refs -> refs.recordView.getAllAsync(null, List.of(KEY_TUPLE))),
    RECORDVIEW_CONTAINS(refs -> refs.recordView.containsAsync(null, KEY_TUPLE)),
    RECORDVIEW_CONTAINS_ALL(refs -> refs.recordView.containsAllAsync(null, List.of(KEY_TUPLE))),
    RECORDVIEW_UPSERT(refs -> refs.recordView.upsertAsync(null, FULL_TUPLE)),
    RECORDVIEW_UPSERT_ALL(refs -> refs.recordView.upsertAllAsync(null, List.of(FULL_TUPLE))),
    RECORDVIEW_GET_AND_UPSERT(refs -> refs.recordView.getAndUpsertAsync(null, FULL_TUPLE)),
    RECORDVIEW_INSERT(refs -> refs.recordView.insertAsync(null, FULL_TUPLE)),
    RECORDVIEW_INSERT_ALL(refs -> refs.recordView.insertAllAsync(null, List.of(FULL_TUPLE))),
    RECORDVIEW_REPLACE(refs -> refs.recordView.replaceAsync(null, FULL_TUPLE)),
    RECORDVIEW_REPLACE_EXACT(refs -> refs.recordView.replaceAsync(null, FULL_TUPLE, FULL_TUPLE)),
    RECORDVIEW_GET_AND_REPLACE(refs -> refs.recordView.getAndReplaceAsync(null, FULL_TUPLE)),
    RECORDVIEW_DELETE(refs -> refs.recordView.deleteAsync(null, KEY_TUPLE)),
    RECORDVIEW_DELETE_EXACT(refs -> refs.recordView.deleteExactAsync(null, FULL_TUPLE)),
    RECORDVIEW_GET_AND_DELETE(refs -> refs.recordView.getAndDeleteAsync(null, KEY_TUPLE)),
    RECORDVIEW_DELETE_ALL(refs -> refs.recordView.deleteAllAsync(null, List.of(KEY_TUPLE))),
    RECORDVIEW_DELETE_ALL_EXACT(refs -> refs.recordView.deleteAllExactAsync(null, List.of(FULL_TUPLE))),
    RECORDVIEW_STREAM_DATA(refs -> {
        CompletableFuture<?> future;
        try (var publisher = new SimplePublisher<Tuple>()) {
            future = refs.recordView.streamData(publisher, null);
            publisher.submit(FULL_TUPLE);
        }
        return future;
    }),
    RECORDVIEW_QUERY(refs -> refs.recordView.queryAsync(null, null)),
    RECORDVIEW_QUERY_WITH_INDEX(refs -> refs.recordView.queryAsync(null, null, null)),
    RECORDVIEW_QUERY_WITH_OPTIONS(refs -> refs.recordView.queryAsync(null, null, null, null)),

    TYPED_RECORDVIEW_GET(refs -> refs.typedRecordView.getAsync(null, new Record(1, ""))),

    MAPPED_RECORDVIEW_GET(refs -> refs.mappedRecordView.getAsync(null, new Record(1, ""))),

    PARTITION_MANAGER_PRIMARY_REPLICA(refs -> refs.partitionManager.primaryReplicaAsync(new HashPartition(0))),
    PARTITION_MANAGER_PRIMARY_REPLICAS(refs -> refs.partitionManager.primaryReplicasAsync()),
    PARTITION_MANAGER_PARTITION_ASYNC_BY_KEY(refs -> refs.partitionManager.partitionAsync(1, Mapper.of(Integer.class))),
    PARTITION_MANAGER_PARTITION_ASYNC_BY_TUPLE(refs -> refs.partitionManager.partitionAsync(KEY_TUPLE));

    private final Function<References, CompletableFuture<?>> action;

    AsyncApiOperation(Function<References, CompletableFuture<?>> action) {
        this.action = action;
    }

    CompletableFuture<?> execute(References references) {
        return action.apply(references);
    }
}
