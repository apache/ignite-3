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

package org.apache.ignite.internal.storage.rocksdb.instance;

import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.KEY_BYTE_ORDER;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksIterator;

/**
 * Cursor that iterates over distinct index IDs in a Sorted Index column family.
 *
 * <p>Sorted index column family can contain multiple indexes that have the same order and type of indexed columns. Index ID is stored
 * as the first 4 bytes of a RocksDB key.
 */
class IndexIdCursor implements Cursor<Integer> {
    private final RocksIterator it;

    private @Nullable ByteBuffer curIndexId = null;

    private boolean hasNext = true;

    IndexIdCursor(RocksIterator it) {
        this.it = it;
    }

    @Override
    public boolean hasNext() {
        if (!hasNext) {
            return false;
        }

        if (curIndexId == null) {
            curIndexId = ByteBuffer.allocate(Integer.BYTES).order(KEY_BYTE_ORDER);

            it.seekToFirst();
        } else if (setNextIndexId()) {
            it.seek(curIndexId);

            curIndexId.rewind();
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

    @Override
    public Integer next() {
        if (!hasNext) {
            throw new NoSuchElementException();
        }

        assert curIndexId != null;

        it.key(curIndexId);

        curIndexId.rewind();

        return curIndexId.getInt(0);
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
