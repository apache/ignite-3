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

package org.apache.ignite.storage.impl;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.schema.TableSchemaManager;
import org.apache.ignite.internal.storage.TableRow;
import org.apache.ignite.storage.TableStorage;

/**
 * Dummy table implementation.
 */
public class DummyTableStorageImpl implements TableStorage {
    /** In-memory dummy store. */
    private final Map<BinaryObjWrapper, BinaryObjWrapper> store = new ConcurrentHashMap<>();

    /** Schema manager. */
    private TableSchemaManager schemaMgr;

    /** {@inheritDoc} */
    @Override public TableRow get(TableRow obj) {
        BinaryObjWrapper row = store.get(new BinaryObjWrapper(obj.getKeyBytes()));

        return new DummyTableRowImpl(row.data.clone()); // Clone.
    }

    /** {@inheritDoc} */
    @Override public TableRow put(TableRow row) {
        final BinaryObjWrapper old = store.put(new BinaryObjWrapper(row.getKeyBytes()), new BinaryObjWrapper(row.getBytes()));

        return old == null ? null : new DummyTableRowImpl(old.data.clone());
    }

    /**
     * Wrapper provides correct byte[] comparison.
     */
    private static class BinaryObjWrapper {
        /** Data. */
        private final byte[] data;

        /**
         * Constructor.
         *
         * @param data Wrapped data.
         */
        BinaryObjWrapper(byte[] data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            BinaryObjWrapper wrapper = (BinaryObjWrapper)o;
            return Arrays.equals(data, wrapper.data);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Arrays.hashCode(data);
        }
    }
}
