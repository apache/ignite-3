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

package org.apache.ignite.internal.table.metrics;

/**
 * Table metrics handler that delegates actual metrics recording to the given {@link TableMetricSource} instance.
 */
public class TableMetrics {
    /** Delegate that records metrics. */
    private volatile TableMetricSource delegate;

    /**
     * Creates a new instance of wrapper based on the given metric source.
     *
     * @param delegate Delegate that records metrics.
     */
    public TableMetrics(TableMetricSource delegate) {
        this.delegate = delegate;
    }

    /**
     * Increments a counter of reads.
     *
     * @param readOnly {@code true} if read operation is executed within read-only transaction, and {@code false} otherwise.
     * @see TableMetricSource#onRead(boolean)
     */
    public void onRead(boolean readOnly) {
        delegate.onRead(readOnly);
    }

    /**
     * Adds the given {@code x} to a counter of reads.
     *
     * @param readOnly {@code true} if read operation is executed within read-only transaction, and {@code false} otherwise.
     * @see TableMetricSource#onRead(int, boolean)
     */
    public void onRead(int x, boolean readOnly) {
        delegate.onRead(x, readOnly);
    }

    /**
     * Increments a counter of writes.
     *
     * @see TableMetricSource#onWrite()
     */
    public void onWrite() {
        delegate.onWrite();
    }

    /**
     * Adds the given {@code x} to a counter of writes.
     *
     * @param x Value to add.
     * @see TableMetricSource#onWrite(int)
     */
    public void onWrite(int x) {
        delegate.onWrite(x);
    }

    /**
     * Replaces the current delegate with the given one.
     *
     * @param delegate New delegate that records metrics.
     */
    public void replaceDelegate(TableMetricSource delegate) {
        this.delegate = delegate;
    }

    /**
     * Returns the current delegate that records metrics.
     *
     * @return Current delegate that records metrics.
     */
    public TableMetricSource delegate() {
        return delegate;
    }
}
