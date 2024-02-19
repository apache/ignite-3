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

package org.apache.ignite.internal.table;

import static java.util.concurrent.CompletableFuture.allOf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.marshaller.reflection.KvMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.table.IndexWrapper.HashIndexWrapper;
import org.apache.ignite.internal.table.IndexWrapper.SortedIndexWrapper;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.PartitionSet;
import org.apache.ignite.internal.table.distributed.TableIndexStoragesSupplier;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.TestOnly;

/**
 * Table view implementation for binary objects.
 */
public class TableImpl implements TableViewInternal {
    /** Internal table. */
    private final InternalTable tbl;

    private final LockManager lockManager;

    private final SchemaVersions schemaVersions;

    /** Ignite SQL facade. */
    private final IgniteSql sql;

    /** Schema registry. Should be set either in constructor or via {@link #schemaView(SchemaRegistry)} before start of using the table. */
    private volatile SchemaRegistry schemaReg;

    private final CompletableFuture<Integer> pkId = new CompletableFuture<>();

    private final Map<Integer, CompletableFuture<?>> indexesToWait = new ConcurrentHashMap<>();

    private final Map<Integer, IndexWrapper> indexWrapperById = new ConcurrentHashMap<>();

    private final MarshallersProvider marshallers;

    /**
     * Constructor.
     *
     * @param tbl The table.
     * @param lockManager Lock manager.
     * @param schemaVersions Schema versions access.
     * @param marshallers Marshallers provider.
     * @param sql Ignite SQL facade.
     */
    public TableImpl(
            InternalTable tbl,
            LockManager lockManager,
            SchemaVersions schemaVersions,
            MarshallersProvider marshallers,
            IgniteSql sql
    ) {
        this.tbl = tbl;
        this.lockManager = lockManager;
        this.schemaVersions = schemaVersions;
        this.marshallers = marshallers;
        this.sql = sql;
    }

    /**
     * Constructor.
     *
     * @param tbl The table.
     * @param schemaReg Table schema registry.
     * @param lockManager Lock manager.
     * @param schemaVersions Schema versions access.
     * @param sql Ignite SQL facade.
     */
    @TestOnly
    public TableImpl(InternalTable tbl, SchemaRegistry schemaReg, LockManager lockManager, SchemaVersions schemaVersions, IgniteSql sql) {
        this(tbl, lockManager, schemaVersions, new ReflectionMarshallersProvider(), sql);

        this.schemaReg = schemaReg;
    }

    @Override
    public int tableId() {
        return tbl.tableId();
    }

    @Override
    public void pkId(int pkId) {
        this.pkId.complete(pkId);
    }

    @Override
    public int pkId() {
        return pkId.join();
    }

    @Override
    public InternalTable internalTable() {
        return tbl;
    }

    @Override public String name() {
        return tbl.name();
    }

    // TODO: revisit this approach, see https://issues.apache.org/jira/browse/IGNITE-21235.
    public void name(String newName) {
        tbl.name(newName);
    }

    @Override
    public SchemaRegistry schemaView() {
        return schemaReg;
    }

    @Override
    public void schemaView(SchemaRegistry schemaReg) {
        assert this.schemaReg == null : "Schema registry is already set [tableName=" + name() + ']';

        Objects.requireNonNull(schemaReg, "Schema registry must not be null [tableName=" + name() + ']');

        this.schemaReg = schemaReg;
    }

    @Override
    public <R> RecordView<R> recordView(Mapper<R> recMapper) {
        return new RecordViewImpl<>(tbl, schemaReg, schemaVersions, marshallers, recMapper, sql);
    }

    @Override
    public RecordView<Tuple> recordView() {
        return new RecordBinaryViewImpl(tbl, schemaReg, schemaVersions, marshallers, sql);
    }

    @Override
    public <K, V> KeyValueView<K, V> keyValueView(Mapper<K> keyMapper, Mapper<V> valMapper) {
        return new KeyValueViewImpl<>(tbl, schemaReg, schemaVersions, marshallers, sql, keyMapper, valMapper);
    }

    @Override
    public KeyValueView<Tuple, Tuple> keyValueView() {
        return new KeyValueBinaryViewImpl(tbl, schemaReg, schemaVersions, marshallers, sql);
    }

    @Override
    public int partition(Tuple key) {
        Objects.requireNonNull(key);

        try {
            // Taking latest schema version for marshaller here because it's only used to calculate colocation hash, and colocation
            // columns never change (so they are the same for all schema versions of the table),
            Row keyRow = new TupleMarshallerImpl(schemaReg.lastKnownSchema()).marshalKey(key);

            return tbl.partition(keyRow);
        } catch (TupleMarshallerException e) {
            throw new IgniteInternalException(e);
        }
    }

    @Override
    public <K> int partition(K key, Mapper<K> keyMapper) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(keyMapper);

        BinaryRowEx keyRow;
        var marshaller = new KvMarshallerImpl<>(schemaReg.lastKnownSchema(), marshallers, keyMapper, keyMapper);
        try {
            keyRow = marshaller.marshal(key);
        } catch (MarshallerException e) {
            throw new IgniteInternalException("Cannot marshal key", e);
        }

        return tbl.partition(keyRow);
    }

    @Override
    public ClusterNode leaderAssignment(int partition) {
        return tbl.leaderAssignment(partition);
    }

    /** Returns a supplier of index storage wrapper factories for given partition. */
    public TableIndexStoragesSupplier indexStorageAdapters(int partId) {
        return new TableIndexStoragesSupplier() {
            @Override
            public Map<Integer, TableSchemaAwareIndexStorage> get() {
                awaitIndexes();

                List<IndexWrapper> factories = new ArrayList<>(indexWrapperById.values());

                Map<Integer, TableSchemaAwareIndexStorage> adapters = new HashMap<>();

                for (IndexWrapper factory : factories) {
                    TableSchemaAwareIndexStorage storage = factory.getStorage(partId);
                    adapters.put(storage.id(), storage);
                }

                return adapters;
            }

            @Override
            public void addIndexToWaitIfAbsent(int indexId) {
                addIndexesToWait(indexId);
            }
        };
    }

    /** Returns a supplier of index locker factories for given partition. */
    public Supplier<Map<Integer, IndexLocker>> indexesLockers(int partId) {
        return () -> {
            awaitIndexes();

            List<IndexWrapper> factories = new ArrayList<>(indexWrapperById.values());

            Map<Integer, IndexLocker> lockers = new HashMap<>(factories.size());

            for (IndexWrapper factory : factories) {
                IndexLocker locker = factory.getLocker(partId);
                lockers.put(locker.id(), locker);
            }

            return lockers;
        };
    }

    /**
     * The future completes when the primary key index is ready to use.
     *
     * @return Future which complete when a primary index for the table is .
     */
    public CompletableFuture<Void> pkIndexesReadyFuture() {
        var fut = new CompletableFuture<Void>();

        pkId.whenComplete((uuid, throwable) -> fut.complete(null));

        return fut;
    }

    @Override
    public void registerHashIndex(
            StorageHashIndexDescriptor indexDescriptor,
            boolean unique,
            ColumnsExtractor searchRowResolver,
            PartitionSet partitions
    ) {
        int indexId = indexDescriptor.id();

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19112 Create storages once.
        partitions.stream().forEach(partitionId -> {
            tbl.storage().getOrCreateHashIndex(partitionId, indexDescriptor);
        });

        indexWrapperById.put(indexId, new HashIndexWrapper(tbl, lockManager, indexId, searchRowResolver, unique));

        completeWaitIndex(indexId);
    }

    @Override
    public void registerSortedIndex(
            StorageSortedIndexDescriptor indexDescriptor,
            ColumnsExtractor searchRowResolver,
            PartitionSet partitions
    ) {
        int indexId = indexDescriptor.id();

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19112 Create storages once.
        partitions.stream().forEach(partitionId -> {
            tbl.storage().getOrCreateSortedIndex(partitionId, indexDescriptor);
        });

        indexWrapperById.put(indexId, new SortedIndexWrapper(tbl, lockManager, indexId, searchRowResolver));

        completeWaitIndex(indexId);
    }

    @Override
    public void unregisterIndex(int indexId) {
        indexWrapperById.remove(indexId);

        completeWaitIndex(indexId);

        // TODO: IGNITE-20121 Also need to destroy the index storages
    }

    private void awaitIndexes() {
        List<CompletableFuture<?>> toWait = new ArrayList<>();

        toWait.add(pkId);

        toWait.addAll(indexesToWait.values());

        allOf(toWait.toArray(CompletableFuture[]::new)).join();
    }

    /**
     * Prepares this table for being closed.
     */
    public void beforeClose() {
        IgniteInternalException closeTableException = new IgniteInternalException(
                ErrorGroups.Table.TABLE_STOPPING_ERR,
                "Table is being stopped: tableId=" + tableId()
        );

        pkId.completeExceptionally(closeTableException);

        indexesToWait.values().forEach(future -> future.completeExceptionally(closeTableException));
    }

    /**
     * Adds indexes to wait, if not already created, before inserting data into the table.
     *
     * @param indexIds Indexes Index IDs.
     */
    // TODO: IGNITE-19082 Needs to be redone/improved
    public void addIndexesToWait(int... indexIds) {
        for (int indexId : indexIds) {
            indexesToWait.compute(indexId, (indexId0, awaitIndexFuture) -> {
                if (awaitIndexFuture != null) {
                    return awaitIndexFuture;
                }

                if (indexWrapperById.containsKey(indexId)) {
                    // Index is already registered and created.
                    return null;
                }

                return new CompletableFuture<>();
            });
        }
    }

    private void completeWaitIndex(int indexId) {
        CompletableFuture<?> indexToWaitFuture = indexesToWait.remove(indexId);

        if (indexToWaitFuture != null) {
            indexToWaitFuture.complete(null);
        }
    }
}
