/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.index.impl;

import static org.apache.ignite.internal.util.IgniteUtils.capacity;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.util.Cursor;

/**
 * Test-only implementation of a {@link HashIndexStorage}.
 */
public class TestHashIndexStorage implements HashIndexStorage {
    private final ConcurrentMap<ByteBuffer, Set<RowId>> index = new ConcurrentHashMap<>();

    private final HashIndexDescriptor descriptor;

    /**
     * Constructor.
     */
    public TestHashIndexStorage(HashIndexDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public HashIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) {
        Collection<RowId> rowIds = index.getOrDefault(key.byteBuffer(), Set.of());

        return Cursor.fromIterator(rowIds.iterator());
    }

    @Override
    public void put(IndexRow row) {
        index.compute(row.indexColumns().byteBuffer(), (k, v) -> {
            if (v == null) {
                return Set.of(row.rowId());
            } else if (v.contains(row.rowId())) {
                return v;
            } else {
                var result = new HashSet<RowId>(capacity(v.size() + 1));

                result.addAll(v);
                result.add(row.rowId());

                return result;
            }
        });
    }

    @Override
    public void remove(IndexRow row) {
        index.computeIfPresent(row.indexColumns().byteBuffer(), (k, v) -> {
            if (v.contains(row.rowId())) {
                if (v.size() == 1) {
                    return null;
                } else {
                    var result = new HashSet<>(v);

                    result.remove(row.rowId());

                    return result;
                }
            } else {
                return v;
            }
        });
    }

    @Override
    public void destroy() {
        index.clear();
    }
}
