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

package org.apache.ignite.internal.storage.rocksdb;

import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.KEY_BYTE_ORDER;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.storage.rocksdb.IndexIdCursor.TableAndIndexId;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksIterator;

/**
 * Cursor that iterates over distinct index IDs in a Sorted Index column family.
 *
 * <p>Sorted index column family can contain multiple indexes that have the same order and type of indexed columns. Index ID is stored
 * as the second 4 bytes of a RocksDB key.
 */
public class IndexIdCursor implements Cursor<TableAndIndexId> {
    /**
     * Container for a table ID and an index ID.
     */
    public static class TableAndIndexId {
        static final int BYTES = Integer.BYTES * 2;

        private final int tableId;

        private final int indexId;

        TableAndIndexId(int tableId, int indexId) {
            this.tableId = tableId;
            this.indexId = indexId;
        }

        public int tableId() {
            return tableId;
        }

        public int indexId() {
            return indexId;
        }
    }

    private final RocksIterator it;

    @Nullable
    private final Integer tableId;

    private @Nullable ByteBuffer curIndexId = null;

    private boolean hasNext = true;

    /**
     * Creates a new cursor.
     *
     * @param it RocksDB iterator over a Sorted Index Column Family.
     * @param tableId Table ID filter. If set, the cursor will only return index IDs that belong to the given table. If {@code null}
     *         - all index IDs will be returned.
     */
    public IndexIdCursor(RocksIterator it, @Nullable Integer tableId) {
        this.it = it;
        this.tableId = tableId;
    }

    @Override
    public boolean hasNext() {
        if (!hasNext) {
            return false;
        }

        if (curIndexId == null) {
            positionToFirst();
        } else if (setNextIndexId()) {
            positionToNext();
        } else {
            hasNext = false;

            return false;
        }

        hasNext = it.isValid();

        if (!hasNext) {
            RocksUtils.checkIterator(it);
        }

        return hasNext;
    }

    private void positionToFirst() {
        curIndexId = ByteBuffer.allocate(TableAndIndexId.BYTES).order(KEY_BYTE_ORDER);

        if (tableId != null) {
            curIndexId.putInt(0, tableId);

            it.seek(curIndexId);

            curIndexId.rewind();
        } else {
            it.seekToFirst();
        }
    }

    private void positionToNext() {
        assert curIndexId != null;

        it.seek(curIndexId);

        curIndexId.rewind();
    }

    @Override
    public TableAndIndexId next() {
        if (!hasNext) {
            throw new NoSuchElementException();
        }

        assert curIndexId != null;

        it.key(curIndexId);

        curIndexId.rewind();

        return new TableAndIndexId(
                curIndexId.getInt(0),
                curIndexId.getInt(Integer.BYTES)
        );
    }

    @Override
    public void close() {
        it.close();
    }

    /**
     * Sets the next hypothetical index ID by incrementing the current index ID by one.
     *
     * @return {@code false} if the current index ID already has the maximum possible value and the ID range has been exhausted.
     *         {@code true} otherwise.
     */
    private boolean setNextIndexId() {
        assert curIndexId != null;

        for (int i = curIndexId.remaining() - 1; i >= 0; i--) {
            byte b = (byte) (curIndexId.get(i) + 1);

            curIndexId.put(i, b);

            if (b != 0) {
                return true;
            }
        }

        return false;
    }
}
