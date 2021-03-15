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
import java.util.List;
import java.util.Map;
import org.apache.ignite.table.BinaryKVInvokeProcessor;
import org.apache.ignite.table.KVBinaryView;
import org.apache.ignite.table.KVFacade;
import org.apache.ignite.table.binary.ColSpan;
import org.apache.ignite.table.binary.ColSpanBuilder;

public class KVBinaryImpl implements KVBinaryView {
    /**
     * Constructor.
     *
     * @param tbl Table storage.
     */
    public KVBinaryImpl(TableStorage tbl) {
    }

    /** {@inheritDoc} */
    @Override public ColSpan get(ColSpan key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<ColSpan> getAll(Collection<ColSpan> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(ColSpan key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void put(ColSpan key, ColSpan val) {

    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<ColSpan, ColSpan> pairs) {

    }

    /** {@inheritDoc} */
    @Override public ColSpan getAndPut(ColSpan key, ColSpan val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(ColSpan key, ColSpan val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(ColSpan key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(ColSpan key, ColSpan val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Collection<ColSpan> removeAll(Collection<ColSpan> keys) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public ColSpan getAndRemove(ColSpan key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(ColSpan key, ColSpan val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(ColSpan key, ColSpan oldVal, ColSpan newVal) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public ColSpan getAndReplace(ColSpan key, ColSpan val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> R invoke(
        ColSpan key,
        BinaryKVInvokeProcessor<R> proc,
        Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> List<R> invokeAll(
        List<ColSpan> keys,
        BinaryKVInvokeProcessor<R> proc,
        Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ColSpanBuilder colSpanBuilder() {
        return null;
    }
}
