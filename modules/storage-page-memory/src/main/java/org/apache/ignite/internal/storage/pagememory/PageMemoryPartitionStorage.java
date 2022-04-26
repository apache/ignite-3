/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.storage.StorageUtils.groupId;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.IgniteTree;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.InvokeClosure;
import org.apache.ignite.internal.storage.OperationType;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteCursor;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Storage implementation based on a {@link BplusTree}.
 */
// TODO: IGNITE-16644 Support snapshots.
class PageMemoryPartitionStorage implements PartitionStorage {
    private final int partId;

    private final TableTree tree;

    private final TableFreeList freeList;

    /**
     * Constructor.
     *
     * @param partId Partition id.
     * @param tableCfg – Table configuration.
     * @param dataRegion – Data region for the table.
     * @param freeList Table free list.
     * @throws StorageException If there is an error while creating the partition storage.
     */
    public PageMemoryPartitionStorage(
            int partId,
            TableConfiguration tableCfg,
            AbstractPageMemoryDataRegion dataRegion,
            TableFreeList freeList
    ) throws StorageException {
        assert partId >= 0 && partId < MAX_PARTITION_ID : partId;

        this.partId = partId;

        this.freeList = freeList;

        TableView tableView = tableCfg.value();

        int grpId = groupId(tableView);

        try {
            // TODO: IGNITE-16641 It is necessary to do getting the tree root for the persistent case.
            long metaPageId = dataRegion.pageMemory().allocatePage(grpId, partId, FLAG_AUX);

            // TODO: IGNITE-16641 It is necessary to take into account the persistent case.
            boolean initNew = true;

            tree = new TableTree(
                    grpId,
                    tableView.name(),
                    dataRegion.pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    metaPageId,
                    freeList,
                    partId,
                    initNew
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error occurred while creating the partition storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int partitionId() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable DataRow read(SearchRow key) throws StorageException {
        try {
            return wrap(tree.findOne(wrap(key)));
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error reading row", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Collection<DataRow> readAll(List<? extends SearchRow> keys) throws StorageException {
        Collection<DataRow> res = new ArrayList<>(keys.size());

        try {
            for (SearchRow key : keys) {
                res.add(wrap(tree.findOne(wrap(key))));
            }
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error reading rows", e);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public void write(DataRow row) throws StorageException {
        try {
            TableDataRow dataRow = wrap(row);

            freeList.insertDataRow(dataRow);

            tree.put(dataRow);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error writing row", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeAll(List<? extends DataRow> rows) throws StorageException {
        try {
            for (DataRow row : rows) {
                TableDataRow dataRow = wrap(row);

                freeList.insertDataRow(dataRow);

                tree.put(dataRow);
            }
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error writing rows", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Collection<DataRow> insertAll(List<? extends DataRow> rows) throws StorageException {
        Collection<DataRow> cantInsert = new ArrayList<>();

        try {
            InsertClosure insertClosure = new InsertClosure(freeList);

            for (DataRow row : rows) {
                TableDataRow dataRow = wrap(row);

                insertClosure.reset();

                insertClosure.newRow = dataRow;

                tree.invoke(dataRow, null, insertClosure);

                if (insertClosure.oldRow != null) {
                    cantInsert.add(row);
                }
            }
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error inserting rows", e);
        }

        return cantInsert;
    }

    /** {@inheritDoc} */
    @Override
    public void remove(SearchRow key) throws StorageException {
        try {
            TableSearchRow searchRow = wrap(key);

            TableDataRow removed = tree.remove(searchRow);

            if (removed != null) {
                freeList.removeDataRowByLink(removed.link());
            }
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error removing row", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Collection<SearchRow> removeAll(List<? extends SearchRow> keys) throws StorageException {
        Collection<SearchRow> skippedRows = new ArrayList<>();

        try {
            for (SearchRow key : keys) {
                TableDataRow removed = tree.remove(wrap(key));

                if (removed != null) {
                    freeList.removeDataRowByLink(removed.link());
                } else {
                    skippedRows.add(key);
                }
            }
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error removing rows", e);
        }

        return skippedRows;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<DataRow> removeAllExact(List<? extends DataRow> keyValues) throws StorageException {
        Collection<DataRow> skipped = new ArrayList<>();

        try {
            RemoveExactClosure removeExactClosure = new RemoveExactClosure();

            for (DataRow keyValue : keyValues) {
                TableDataRow dataRow = wrap(keyValue);

                removeExactClosure.reset();

                removeExactClosure.forRemoveRow = dataRow;

                tree.invoke(dataRow, null, removeExactClosure);

                if (removeExactClosure.foundRow == null) {
                    skipped.add(keyValue);
                } else {
                    freeList.removeDataRowByLink(removeExactClosure.foundRow.link());
                }
            }
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error while removing exact rows", e);
        }

        return skipped;
    }

    /** {@inheritDoc} */
    @Override
    public <T> @Nullable T invoke(SearchRow key, InvokeClosure<T> clo) throws StorageException {
        IgniteTree.InvokeClosure<TableDataRow> treeClosure = new IgniteTree.InvokeClosure<>() {
            /** {@inheritDoc} */
            @Override
            public void call(@Nullable TableDataRow oldRow) {
                clo.call(wrap(oldRow));
            }

            /** {@inheritDoc} */
            @Override
            public @Nullable TableDataRow newRow() {
                DataRow newRow = clo.newRow();

                if (newRow == null) {
                    return null;
                }

                TableDataRow dataRow = wrap(newRow);

                try {
                    freeList.insertDataRow(dataRow);
                } catch (IgniteInternalCheckedException e) {
                    throw new IgniteInternalException(e);
                }

                return dataRow;
            }

            /** {@inheritDoc} */
            @Override
            public IgniteTree.OperationType operationType() {
                OperationType operationType = clo.operationType();

                switch (operationType) {
                    case WRITE:
                        return IgniteTree.OperationType.PUT;

                    case REMOVE:
                        return IgniteTree.OperationType.REMOVE;

                    case NOOP:
                        return IgniteTree.OperationType.NOOP;

                    default:
                        throw new UnsupportedOperationException(String.valueOf(clo.operationType()));
                }
            }
        };

        try {
            tree.invoke(wrap(key), null, treeClosure);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error invoking a closure for a row", e);
        }

        return clo.result();
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<DataRow> scan(Predicate<SearchRow> filter) throws StorageException {
        try {
            IgniteCursor<TableDataRow> treeCursor = tree.find(null, null);

            return new Cursor<DataRow>() {
                @Nullable TableDataRow cur = advance();

                /** {@inheritDoc} */
                @Override
                public void close() {
                }

                /** {@inheritDoc} */
                @Override
                public Iterator<DataRow> iterator() {
                    return this;
                }

                /** {@inheritDoc} */
                @Override
                public boolean hasNext() {
                    return cur != null;
                }

                /** {@inheritDoc} */
                @Override
                public DataRow next() {
                    DataRow next = wrap(cur);

                    if (next == null) {
                        throw new NoSuchElementException();
                    }

                    try {
                        cur = advance();
                    } catch (IgniteInternalCheckedException e) {
                        throw new StorageException("Error getting next row", e);
                    }

                    return next;
                }

                @Nullable TableDataRow advance() throws IgniteInternalCheckedException {
                    while (treeCursor.next()) {
                        TableDataRow dataRow = treeCursor.get();

                        if (filter.test(wrap(dataRow))) {
                            return dataRow;
                        }
                    }

                    return null;
                }
            };
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error while scanning rows", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> snapshot(Path snapshotPath) {
        throw new UnsupportedOperationException("Snapshots are not supported yet.");
    }

    /** {@inheritDoc} */
    @Override
    public void restoreSnapshot(Path snapshotPath) {
        throw new UnsupportedOperationException("Snapshots are not supported yet.");
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() throws StorageException {
        try {
            tree.destroy();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error while destroying data", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public long rowsCount() {
        try {
            return tree.size();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error occurred while fetching the size.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        tree.close();
    }

    private static TableSearchRow wrap(SearchRow searchRow) {
        ByteBuffer key = searchRow.key();

        return new TableSearchRow(StorageUtils.hashCode(key), key);
    }

    private static TableDataRow wrap(DataRow dataRow) {
        ByteBuffer key = dataRow.key();
        ByteBuffer value = dataRow.value();

        return new TableDataRow(StorageUtils.hashCode(key), key, value);
    }

    private static @Nullable DataRow wrap(TableDataRow tableDataRow) {
        return tableDataRow == null ? null : new TableDataRowAdapter(tableDataRow);
    }

    private static class InsertClosure implements IgniteTree.InvokeClosure<TableDataRow> {
        final TableFreeList freeList;

        TableDataRow newRow;

        @Nullable TableDataRow oldRow;

        InsertClosure(TableFreeList freeList) {
            this.freeList = freeList;
        }

        /** {@inheritDoc} */
        @Override
        public void call(@Nullable TableDataRow oldRow) {
            this.oldRow = oldRow;
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable TableDataRow newRow() {
            assert newRow != null;

            try {
                freeList.insertDataRow(newRow);
            } catch (IgniteInternalCheckedException e) {
                throw new IgniteInternalException(e);
            }

            return newRow;
        }

        /** {@inheritDoc} */
        @Override
        public IgniteTree.OperationType operationType() {
            return oldRow == null ? IgniteTree.OperationType.PUT : IgniteTree.OperationType.NOOP;
        }

        void reset() {
            newRow = null;

            oldRow = null;
        }
    }

    private static class RemoveExactClosure implements IgniteTree.InvokeClosure<TableDataRow> {
        TableDataRow forRemoveRow;

        @Nullable TableDataRow foundRow;

        /** {@inheritDoc} */
        @Override
        public void call(@Nullable TableDataRow oldRow) {
            assert forRemoveRow != null;

            if (oldRow != null && oldRow.value().equals(forRemoveRow.value())) {
                foundRow = oldRow;
            }
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable TableDataRow newRow() {
            return null;
        }

        /** {@inheritDoc} */
        @Override
        public IgniteTree.OperationType operationType() {
            return foundRow == null ? IgniteTree.OperationType.NOOP : IgniteTree.OperationType.REMOVE;
        }

        void reset() {
            forRemoveRow = null;

            foundRow = null;
        }
    }
}
