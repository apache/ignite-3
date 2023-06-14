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

package org.apache.ignite.internal.rocksdb;

import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.rocksdb.RocksIterator;

/**
 * Adapter from a {@link RocksIterator} to a {@link Cursor} that also handles stopping of the storage.
 */
public abstract class BusyRocksIteratorAdapter<T> extends RocksIteratorAdapter<T> {
    /** Busy lock. */
    private final IgniteSpinBusyLock busyLock;

    /**
     * Constructor.
     *
     * @param busyLock Busy lock.
     * @param it RocksDB iterator.
     */
    protected BusyRocksIteratorAdapter(IgniteSpinBusyLock busyLock, RocksIterator it) {
        super(it);
        this.busyLock = busyLock;
    }

    /**
     * Handles busy lock acquiring failure. This means that db has been stopped and cursor can't proceed. Must throw an exception.
     */
    protected abstract void handleBusyFail();

    /**
     * Handles busy lock acquiring success.
     */
    protected void handeBusySuccess() {
        // No-op.
    }

    private void handleBusyFail0() {
        handleBusyFail();

        assert false : "handleBusy() should have thrown an exception.";
    }

    @Override
    public boolean hasNext() {
        if (!busyLock.enterBusy()) {
            handleBusyFail0();
        }

        try {
            handeBusySuccess();

            return super.hasNext();
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public T next() {
        if (!busyLock.enterBusy()) {
            handleBusyFail0();
        }

        try {
            handeBusySuccess();

            return super.next();
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public void close() {
        if (!busyLock.enterBusy()) {
            handleBusyFail0();
        }

        try {
            handeBusySuccess();

            super.close();
        } finally {
            busyLock.leaveBusy();
        }
    }
}
