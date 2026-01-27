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

import org.apache.ignite.internal.metrics.MetricSource;

/** Common interface for reads and writes to tables and caches. */
public interface ReadWriteMetricSource extends MetricSource {
    /**
     * Called when get request found a row.
     *
     * @param readOnly {@code true} if read operation is executed within read-only transaction, and {@code false} otherwise.
     */
    void onReadHit(boolean readOnly);

    /**
     * Called when multi row get request found rows.
     *
     * @param readOnly {@code true} if read operation is executed within read-only transaction, and {@code false} otherwise.
     */
    void onReadHit(int x, boolean readOnly);

    /**
     * Called when get request didn't find a row.
     *
     * @param readOnly {@code true} if read operation is executed within read-only transaction, and {@code false} otherwise.
     */
    void onReadMiss(boolean readOnly);

    /**
     * Called when multi row get request didn't find rows.
     *
     * @param readOnly {@code true} if read operation is executed within read-only transaction, and {@code false} otherwise.
     */
    void onReadMiss(int x, boolean readOnly);

    /**
     * Increments a counter of writes.
     */
    void onWrite();

    /**
     * Adds the given {@code x} to a counter of writes.
     */
    void onWrite(int x);

    /**
     * Should be called instead of {@link #onWrite} if row was removed.
     */
    void onRemoval();

    /**
     * Should be called instead of {@link #onWrite} if row was removed.
     */
    void onRemoval(int x);
}
