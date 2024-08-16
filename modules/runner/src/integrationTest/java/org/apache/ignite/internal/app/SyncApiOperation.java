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
import java.util.function.Consumer;
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
    IGNITE_CLUSTER_NODES(refs -> refs.ignite.clusterNodes()),
    IGNITE_CATALOG(refs -> refs.ignite.catalog()),

    TABLES_TABLES(refs -> refs.tables.tables()),
    TABLES_TABLE(refs -> refs.tables.table(TEST_TABLE_NAME)),

    TABLE_NAME(refs -> refs.table.name()),
    TABLE_KVVIEW(refs -> refs.table.keyValueView()),
    TABLE_TYPED_KVVIEW(refs -> refs.table.keyValueView(Integer.class, String.class)),
    TABLE_MAPPED_KVVIEW(refs -> refs.table.keyValueView(Mapper.of(Integer.class), Mapper.of(String.class))),
    TABLE_RECORDVIEW(refs -> refs.table.recordView()),
    TABLE_TYPED_RECORDVIEW(refs -> refs.table.recordView(Record.class)),
    TABLE_MAPPED_RECORDVIEW(refs -> refs.table.recordView(Mapper.of(Record.class))),
    TABLE_PARTITION_MANAGER(refs -> refs.table.partitionManager()),

    TABLE_FROM_TABLE_ASYNC_PUT(refs -> refs.tableFromTableAsync.keyValueView().put(null, KEY_TUPLE, VALUE_TUPLE)),
    TABLE_FROM_TABLES_PUT(refs -> refs.tableFromTables.keyValueView().put(null, KEY_TUPLE, VALUE_TUPLE)),
    TABLE_FROM_TABLES_ASYNC_PUT(refs -> refs.tableFromTablesAsync.keyValueView().put(null, KEY_TUPLE, VALUE_TUPLE)),

    KVVIEW_GET(refs -> refs.kvView.get(null, KEY_TUPLE)),
    KVVIEW_GET_OR_DEFAULT(refs -> refs.kvView.getOrDefault(null, KEY_TUPLE, null)),
    KVVIEW_GET_ALL(refs -> refs.kvView.getAll(null, List.of(KEY_TUPLE))),
    KVVIEW_CONTAINS(refs -> refs.kvView.contains(null, KEY_TUPLE)),
    KVVIEW_CONTAINS_ALL(refs -> refs.kvView.containsAll(null, List.of(KEY_TUPLE))),
    KVVIEW_PUT(refs -> refs.kvView.put(null, KEY_TUPLE, VALUE_TUPLE)),
    KVVIEW_PUT_ALL(refs -> refs.kvView.putAll(null, Map.of(KEY_TUPLE, VALUE_TUPLE))),
    KVVIEW_GET_AND_PUT(refs -> refs.kvView.getAndPut(null, KEY_TUPLE, VALUE_TUPLE)),
    KVVIEW_PUT_IF_ABSENT(refs -> refs.kvView.putIfAbsent(null, KEY_TUPLE, VALUE_TUPLE)),
    KVVIEW_REMOVE(refs -> refs.kvView.remove(null, KEY_TUPLE)),
    KVVIEW_REMOVE_EXACT(refs -> refs.kvView.remove(null, KEY_TUPLE, VALUE_TUPLE)),
    KVVIEW_REMOVE_ALL(refs -> refs.kvView.removeAll(null, List.of(KEY_TUPLE))),
    KVVIEW_GET_AND_REMOVE(refs -> refs.kvView.getAndRemove(null, KEY_TUPLE)),
    KVVIEW_REPLACE(refs -> refs.kvView.replace(null, KEY_TUPLE, VALUE_TUPLE)),
    KVVIEW_REPLACE_EXACT(refs -> refs.kvView.replace(null, KEY_TUPLE, VALUE_TUPLE, VALUE_TUPLE)),
    KVVIEW_GET_AND_REPLACE(refs -> refs.kvView.getAndReplace(null, KEY_TUPLE, VALUE_TUPLE)),
    KVVIEW_QUERY(refs -> refs.kvView.query(null, null)),
    KVVIEW_QUERY_WITH_INDEX(refs -> refs.kvView.query(null, null, null)),
    KVVIEW_QUERY_WITH_OPTIONS(refs -> refs.kvView.query(null, null, null, null)),

    TYPED_KVVIEW_GET_NULLABLE(refs -> refs.typedKvView.getNullable(null, 1)),
    TYPED_KVVIEW_GET_NULLABLE_AND_PUT(refs -> refs.typedKvView.getNullableAndPut(null, 1, "one")),
    TYPED_KVVIEW_GET_NULLABLE_AND_REMOVE(refs -> refs.typedKvView.getNullableAndRemove(null, 1)),
    TYPED_KVVIEW_GET_NULLABLE_AND_REPLACE(refs -> refs.typedKvView.getNullableAndReplace(null, 1, "one")),

    MAPPED_KVVIEW_GET(refs -> refs.mappedKvView.get(null, 1)),

    RECORDVIEW_GET(refs -> refs.recordView.get(null, KEY_TUPLE)),
    RECORDVIEW_GET_ALL(refs -> refs.recordView.getAll(null, List.of(KEY_TUPLE))),
    RECORDVIEW_CONTAINS(refs -> refs.recordView.contains(null, KEY_TUPLE)),
    RECORDVIEW_CONTAINS_ALL(refs -> refs.recordView.containsAll(null, List.of(KEY_TUPLE))),
    RECORDVIEW_UPSERT(refs -> refs.recordView.upsert(null, FULL_TUPLE)),
    RECORDVIEW_UPSERT_ALL(refs -> refs.recordView.upsertAll(null, List.of(FULL_TUPLE))),
    RECORDVIEW_GET_AND_UPSERT(refs -> refs.recordView.getAndUpsert(null, FULL_TUPLE)),
    RECORDVIEW_INSERT(refs -> refs.recordView.insert(null, FULL_TUPLE)),
    RECORDVIEW_INSERT_ALL(refs -> refs.recordView.insertAll(null, List.of(FULL_TUPLE))),
    RECORDVIEW_REPLACE(refs -> refs.recordView.replace(null, FULL_TUPLE)),
    RECORDVIEW_REPLACE_EXACT(refs -> refs.recordView.replace(null, FULL_TUPLE, FULL_TUPLE)),
    RECORDVIEW_GET_AND_REPLACE(refs -> refs.recordView.getAndReplace(null, FULL_TUPLE)),
    RECORDVIEW_DELETE(refs -> refs.recordView.delete(null, KEY_TUPLE)),
    RECORDVIEW_DELETE_EXACT(refs -> refs.recordView.deleteExact(null, FULL_TUPLE)),
    RECORDVIEW_GET_AND_DELETE(refs -> refs.recordView.getAndDelete(null, KEY_TUPLE)),
    RECORDVIEW_DELETE_ALL(refs -> refs.recordView.deleteAll(null, List.of(KEY_TUPLE))),
    RECORDVIEW_DELETE_ALL_EXACT(refs -> refs.recordView.deleteAllExact(null, List.of(FULL_TUPLE))),

    TYPED_RECORDVIEW_GET(refs -> refs.typedRecordView.get(null, new Record(1, ""))),

    MAPPED_RECORDVIEW_GET(refs -> refs.mappedRecordView.get(null, new Record(1, "")));

    private final Consumer<References> action;

    SyncApiOperation(Consumer<References> action) {
        this.action = action;
    }

    void execute(References references) {
        action.accept(references);
    }
}
