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

package org.apache.ignite.internal.storage.basic;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.InvokeClosure;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.Storage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.NotNull;

/**
 * Storage implementation based on {@link ConcurrentHashMap}.
 */
public class ConcurrentHashMapStorage implements Storage {
    /** Storage content. */
    private final ConcurrentMap<ByteArray, byte[]> map = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public DataRow read(SearchRow key) throws StorageException {
        byte[] keyBytes = key.keyBytes();

        byte[] valueBytes = map.get(new ByteArray(keyBytes));

        return new SimpleDataRow(keyBytes, valueBytes);
    }

    /** {@inheritDoc} */
    @Override public synchronized void write(DataRow row) throws StorageException {
        map.put(new ByteArray(row.keyBytes()), row.valueBytes());
    }

    /** {@inheritDoc} */
    @Override public synchronized void remove(SearchRow key) throws StorageException {
        map.remove(new ByteArray(key.keyBytes()));
    }

    /** {@inheritDoc} */
    @Override public synchronized void invoke(SearchRow key, InvokeClosure clo) throws StorageException {
        byte[] keyBytes = key.keyBytes();

        ByteArray mapKey = new ByteArray(keyBytes);
        byte[] existingDataBytes = map.get(mapKey);

        clo.call(new SimpleDataRow(keyBytes, existingDataBytes));

        switch (clo.operationType()) {
            case WRITE:
                map.put(mapKey, clo.newRow().valueBytes());

                break;

            case REMOVE:
                map.remove(mapKey);

                break;

            case NOOP:
                break;
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor<DataRow> scan(Predicate<SearchRow> filter) throws StorageException {
        Iterator<Map.Entry<ByteArray, byte[]>> iter = map.entrySet().stream()
            .filter(entry -> filter.test(new SimpleDataRow(entry.getKey().bytes(), null)))
            .iterator();

        return new Cursor<DataRow>() {
            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return iter.hasNext();
            }

            /** {@inheritDoc} */
            @Override public DataRow next() {
                Map.Entry<ByteArray, byte[]> next = iter.next();

                return new SimpleDataRow(next.getKey().bytes(), next.getValue());
            }

            /** {@inheritDoc} */
            @NotNull @Override public Iterator<DataRow> iterator() {
                return this;
            }

            /** {@inheritDoc} */
            @Override public void close() throws Exception {
                // No-op.
            }
        };
    }
}
