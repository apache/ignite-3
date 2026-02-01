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

import org.jetbrains.annotations.Nullable;

/**
 * Ignite transaction options.
 */
public class TransactionOptions {
    /** Transaction timeout. 0 means 'use default timeout'. */
    private long timeoutMillis = 0;

    /** Read-only transaction. */
    private boolean readOnly = false;

    /** Transaction label. Used for identification in logs and system views. */
    @Nullable
    private String label = null;

    /**
     * Returns transaction timeout, in milliseconds. 0 means 'use default timeout'.
     *
     * @return Transaction timeout, in milliseconds.
     */
    public long timeoutMillis() {
        return timeoutMillis;
    }

    /**
     * Sets transaction timeout, in milliseconds.
     *
     * @param timeoutMillis Transaction timeout, in milliseconds. Cannot be negative; 0 means 'use default timeout'.
     *      the default timeout is configured via ignite.transaction.timeout configuration property.
     * @return {@code this} for chaining.
     */
    public TransactionOptions timeoutMillis(long timeoutMillis) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("Negative timeoutMillis: " + timeoutMillis);
        }

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
     * Returns transaction label. The label is used for identification in logs and system views.
     *
     * @return Transaction label, or {@code null} if not set.
     */
    @Nullable
    public String label() {
        return label;
    }

    /**
     * Sets transaction label. The label is used for identification in logs and system views.
     *
     * <p>Transaction labels help identify and track transactions in system logs and the transaction system view.
     * They can be useful for debugging and monitoring purposes. As soon as a transaction label is set, it's immutable for the transaction.
     *
     * @param label Transaction label. Can be {@code null} to clear the label.
     * @return {@code this} for chaining.
     */
    public TransactionOptions label(@Nullable String label) {
        this.label = label;

        return this;
    }
}
