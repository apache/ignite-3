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
package org.apache.ignite.raft.jraft.core;

import java.nio.ByteBuffer;
import java.util.function.BooleanSupplier;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Iterator;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;

public class IteratorWrapper implements Iterator {

    private final IteratorImpl impl;
    private final BooleanSupplier shuttingDown;

    public IteratorWrapper(IteratorImpl iterImpl, BooleanSupplier shuttingDown) {
        super();
        this.impl = iterImpl;
        this.shuttingDown = shuttingDown;
    }

    @Override
    public boolean hasNext() {
        return !shuttingDown.getAsBoolean() && iteratorHasNext();
    }

    private boolean iteratorHasNext() {
        return this.impl.isGood() && this.impl.entry().getType() == EnumOutter.EntryType.ENTRY_TYPE_DATA;
    }

    @Override
    public ByteBuffer next() {
        final ByteBuffer data = getData();
        // This method deliberately breaks Iterator#next() contract. It might be called even when it wasn't checked that #hasNext()
        // returns true, and it's not expected to throw a NoSuchElementException. But we need to advance the underlying iterator here
        // (if it can be advanced) even if shutdown was requested, hence we use #iteratorHasNext() instead of #hasNext().
        if (iteratorHasNext()) {
            this.impl.next();
        }
        return data;
    }

    @Override
    public ByteBuffer getData() {
        final LogEntry entry = this.impl.entry();
        return entry != null ? entry.getData() : null;
    }

    @Override
    public long getIndex() {
        return this.impl.getIndex();
    }

    @Override
    public long getTerm() {
        return this.impl.entry().getId().getTerm();
    }

    @Override
    public Closure done() {
        return this.impl.done();
    }

    @Override
    public void setErrorAndRollback(final long ntail, final Status st) {
        this.impl.setErrorAndRollback(ntail, st);
    }
}
