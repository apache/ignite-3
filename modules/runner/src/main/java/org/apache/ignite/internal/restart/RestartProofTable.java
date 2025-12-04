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

package org.apache.ignite.internal.restart;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersionsImpl;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.PartitionDistribution;
import org.apache.ignite.table.partition.PartitionManager;

/**
 * Reference to {@link Table} under a swappable {@link Ignite} instance. When a restart happens, this switches to the new Ignite
 * instance.
 *
 * <p>API operations on this are linearized with respect to node restarts. Normally (except for situations when timeouts trigger), user
 * operations will not interact with detached objects.
 */
class RestartProofTable implements Table, Wrapper {
    private final IgniteAttachmentLock attachmentLock;
    private final int tableId;

    private final RefCache<Table> tableCache;

    /**
     * Constructor.
     */
    RestartProofTable(IgniteAttachmentLock attachmentLock, Ignite initialIgnite, int tableId) {
        this.attachmentLock = attachmentLock;
        this.tableId = tableId;

        tableCache = new RefCache<>(initialIgnite, this::tableFromIgnite);
    }

    static int tableId(Table table) {
        return Wrappers.unwrap(table, TableViewInternal.class).tableId();
    }

    private Table tableFromIgnite(Ignite ignite) {
        Table underlyingTable = ignite.tables().tables().stream()
                .filter(table -> tableId(table) == tableId)
                .findAny()
                .orElse(null);

        if (underlyingTable == null) {
            throw SchemaVersionsImpl.tableNotFoundException(tableId);
        }

        return underlyingTable;
    }

    @Override
    public QualifiedName qualifiedName() {
        return attachmentLock.attached(ignite -> tableCache.actualFor(ignite).qualifiedName());
    }

    @Override
    public PartitionManager partitionManager() {
        return attachmentLock.attached(
                ignite -> new RestartProofPartitionManager(
                        attachmentLock,
                        ignite,
                        currentIgnite -> tableCache.actualFor(currentIgnite).partitionDistribution()
                )
        );
    }

    @Override
    public PartitionDistribution partitionDistribution() {
        return partitionManager();
    }

    @Override
    public <R> RecordView<R> recordView(Mapper<R> recMapper) {
        return attachmentLock.attached(
                ignite -> new RestartProofRecordView<>(
                        attachmentLock,
                        ignite,
                        currentIgnite -> tableCache.actualFor(currentIgnite).recordView(recMapper)
                )
        );
    }

    @Override
    public RecordView<Tuple> recordView() {
        return attachmentLock.attached(
                ignite -> new RestartProofRecordView<>(
                        attachmentLock,
                        ignite,
                        currentIgnite -> tableCache.actualFor(currentIgnite).recordView()
                )
        );
    }

    @Override
    public <K, V> KeyValueView<K, V> keyValueView(Mapper<K> keyMapper, Mapper<V> valMapper) {
        return attachmentLock.attached(
                ignite -> new RestartProofKeyValueView<>(
                        attachmentLock,
                        ignite,
                        currentIgnite -> tableCache.actualFor(currentIgnite).keyValueView(keyMapper, valMapper)
                )
        );
    }

    @Override
    public KeyValueView<Tuple, Tuple> keyValueView() {
        return attachmentLock.attached(
                ignite -> new RestartProofKeyValueView<>(
                        attachmentLock,
                        ignite,
                        currentIgnite -> tableCache.actualFor(currentIgnite).keyValueView()
                )
        );
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return attachmentLock.attached(ignite -> Wrappers.unwrap(tableCache.actualFor(ignite), classToUnwrap));
    }
}
