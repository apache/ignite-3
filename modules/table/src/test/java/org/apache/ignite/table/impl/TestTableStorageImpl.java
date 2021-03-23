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

package org.apache.ignite.table.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.storage.TableStorage;
import org.apache.ignite.internal.table.TableRow;
import org.apache.ignite.internal.table.TableRowAdapter;
import org.apache.ignite.internal.table.TableSchemaManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Dummy table storage implementation.
 */
public class TestTableStorageImpl implements TableStorage {
    /** In-memory dummy store. */
    private final Map<Chunk, Chunk> store = new ConcurrentHashMap<>();

    /** Schema manager. */
    private final TableSchemaManager schemaMgr;

    public TestTableStorageImpl(TableSchemaManager mgr) {
        schemaMgr = mgr;
    }

    /** {@inheritDoc} */
    @Override public TableRow get(@NotNull TableRow obj) {
        Chunk chunk = store.get(new Chunk(obj.keyChunk().toBytes()));

        return chunkToRow(chunk);
    }

    /** {@inheritDoc} */
    @Override public TableRow put(@NotNull TableRow row) {
        if (row.valueChunk() == null) {
            final Chunk old = store.remove(new Chunk(row.keyChunk().toBytes()));

            return chunkToRow(old);
        }

        final Chunk old = store.put(
            new Chunk(row.keyChunk().toBytes()),
            new Chunk(row.toBytes()));

        return chunkToRow(old);
    }

    @Nullable private TableRow chunkToRow(@Nullable Chunk chunk) {
        if (chunk == null)
            return null;

        ByteBuffer buf = ByteBuffer.wrap(chunk.data);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        short ver = buf.getShort(Row.SCHEMA_VERSION_OFFSET);

        final SchemaDescriptor schema = schemaMgr.schema(ver);

        return chunk == null ? null : new TableRowAdapter(new ByteBufferRow(schema, buf), schema);
    }

    /**
     * Wrapper provides correct byte[] comparison.
     */
    private static class Chunk {
        /** Data. */
        private final byte[] data;

        /** Hash. */
        private final int hash;

        /**
         * Constructor.
         *
         * @param data Wrapped data.
         */
        Chunk(byte[] data) {
            this.data = data;
            this.hash = Arrays.hashCode(data);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Chunk wrapper = (Chunk)o;
            return Arrays.equals(data, wrapper.data);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return hash;
        }
    }
}
