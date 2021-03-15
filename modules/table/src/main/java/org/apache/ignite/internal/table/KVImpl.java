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

package org.apache.ignite.internal.table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KV;
import org.apache.ignite.table.binary.BinaryObject;
import org.apache.ignite.table.binary.BinaryObjectBuilder;
import org.apache.ignite.table.binary.Row;

public class KVImpl implements KV {
    /** Underlying storage. */
    private final TableStorage tbl;

    /**
     * Constructor.
     *
     * @param tbl Table storage.
     */
    public KVImpl(TableStorage tbl) {
        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public BinaryObject get(BinaryObject key) {
        Row kRow = toKeyRow(key);

        return tbl.get(kRow);
    }

    /** {@inheritDoc} */
    @Override public Collection<BinaryObject> getAll(Collection<BinaryObject> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(BinaryObject key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void put(BinaryObject key, BinaryObject val) {

    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<BinaryObject, BinaryObject> pairs) {

    }

    /** {@inheritDoc} */
    @Override public BinaryObject getAndPut(BinaryObject key, BinaryObject val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(BinaryObject key, BinaryObject val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(BinaryObject key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(BinaryObject key, BinaryObject val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Collection<BinaryObject> removeAll(Collection<BinaryObject> keys) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public BinaryObject getAndRemove(BinaryObject key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(BinaryObject key, BinaryObject val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(BinaryObject key, BinaryObject oldVal, BinaryObject newVal) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public BinaryObject getAndReplace(BinaryObject key, BinaryObject val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> R invoke(
        BinaryObject key,
        InvokeProcessor<BinaryObject, BinaryObject, R> proc,
        Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> Map<BinaryObject, R> invokeAll(
        Collection<BinaryObject> keys,
        InvokeProcessor<BinaryObject, BinaryObject, R> proc,
        Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder binaryBuilder() {
        return null;
    }

    /**
     * Converts user binary object to row.
     *
     * @param o Binary object.
     * @return Row.
     */
    private Row toKeyRow(BinaryObject o) {
        return null;
    }
}
