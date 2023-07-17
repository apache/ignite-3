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

package org.apache.ignite.tx;

import org.apache.ignite.internal.hlc.HybridTimestamp;

/**
 * Ignite transaction options.
 */
public class TransactionOptions {
    /** Transaction timeout. */
    private long timeoutMillis = 0;

    /** Read-only transaction. */
    private boolean readOnly = false;

    /** Observable timestamp for read-only transactions only. */
    private HybridTimestamp observableTimestamp = null;

    /**
     * Returns transaction timeout, in milliseconds.
     *
     * @return Transaction timeout, in milliseconds.
     */
    public long timeoutMillis() {
        return timeoutMillis;
    }

    /**
     * Sets transaction timeout, in milliseconds.
     *
     * @param timeoutMillis Transaction timeout, in milliseconds.
     * @return {@code this} for chaining.
     */
    public TransactionOptions timeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;

        return this;
    }

    /**
     * Returns a value indicating whether a read-only transaction should be used.
     *
     * <p>Read-only transactions provide a snapshot view of data at a certain point in time.
     * They are lock-free and perform better than normal transactions, but do not permit data modifications.
     *
     * @return Whether a read-only transaction should be used.
     */
    public boolean readOnly() {
        return readOnly;
    }

    /**
     * Sets a value indicating whether a read-only transaction should be used.
     *
     * <p>Read-only transactions provide a snapshot view of data at a certain point in time.
     * They are lock-free and perform better than normal transactions, but do not permit data modifications.
     *
     * @param readOnly Whether a read-only transaction should be used.
     *
     * @return {@code this} for chaining.
     */
    public TransactionOptions readOnly(boolean readOnly) {
        this.readOnly = readOnly;

        return this;
    }

    /**
     * Observable timestamp for read-only transactions only.
     *
     * @return Observable timestamp.
     */
    public HybridTimestamp observableTimestamp() {
        return observableTimestamp;
    }

    /**
     * Set observable timestamp for read-only transactions only.
     *
     * @param observableTimestamp Observable timestamp.
     *
     * @return {@code this} for chaining.
     */
    public TransactionOptions setObservableTimestamp(HybridTimestamp observableTimestamp) {
        assert this.readOnly : "Observable timestamp is applicable only for read-only transactions.";

        this.observableTimestamp = observableTimestamp;

        return this;
    }
}
