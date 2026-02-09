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

package org.apache.ignite.internal.tx;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tx.configuration.TransactionConfigurationSchema;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction options for internal use.
 */
public class InternalTxOptions {
    private static final InternalTxOptions DEFAULT_OPTIONS = builder().build();

    /**
     * Transaction priority. The priority is used to resolve conflicts between transactions. The higher priority is
     * the more likely the transaction will win the conflict.
     */
    private final TxPriority priority;

    /** Transaction timeout. 0 means 'use default timeout'. */
    private final long timeoutMillis;

    /** Transaction label. */
    @Nullable
    private final String txLabel;

    /**
     * Read timestamp of the transaction. If {@code null} - the most recent available timestamp will be calculated based on the current
     * node's clock.
     */
    @Nullable
    private final HybridTimestamp readTimestamp;

    private InternalTxOptions(TxPriority priority, long timeoutMillis, @Nullable HybridTimestamp readTimestamp, @Nullable String txLabel) {
        this.priority = priority;
        this.timeoutMillis = timeoutMillis;
        this.readTimestamp = readTimestamp;
        this.txLabel = txLabel;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static InternalTxOptions defaults() {
        return DEFAULT_OPTIONS;
    }

    public static InternalTxOptions defaultsWithPriority(TxPriority priority) {
        return builder().priority(priority).build();
    }

    public TxPriority priority() {
        return priority;
    }

    public long timeoutMillis() {
        return timeoutMillis;
    }

    public @Nullable HybridTimestamp readTimestamp() {
        return readTimestamp;
    }

    public @Nullable String txLabel() {
        return txLabel;
    }

    /** Builder for InternalTxOptions. */
    public static class Builder {
        private TxPriority priority = TxPriority.NORMAL;

        /**
         * This is NOT actually used as the default timeout, see defaults for {@link TransactionConfigurationSchema#readOnlyTimeoutMillis}
         * and {@link TransactionConfigurationSchema#readWriteTimeoutMillis} which are actually used if tx timeout is 0.
         */
        private long timeoutMillis = 0;

        @Nullable
        private HybridTimestamp readTimestamp = null;

        @Nullable
        private String txLabel = null;

        public Builder priority(TxPriority priority) {
            this.priority = priority;
            return this;
        }

        public Builder timeoutMillis(long timeoutMillis) {
            this.timeoutMillis = timeoutMillis;
            return this;
        }

        public Builder readTimestamp(@Nullable HybridTimestamp readTimestamp) {
            this.readTimestamp = readTimestamp;
            return this;
        }

        public Builder txLabel(@Nullable String txLabel) {
            this.txLabel = txLabel;
            return this;
        }

        public InternalTxOptions build() {
            return new InternalTxOptions(priority, timeoutMillis, readTimestamp, txLabel);
        }
    }
}
