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

package org.apache.ignite.internal.tx.impl;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tostring.S;

/**
 * Read-only transaction ID.
 */
class TxIdAndTimestamp {
    /** Start timestamp of the transaction. */
    private final HybridTimestamp readTimestamp;

    /** Transaction ID. */
    private final UUID txId;

    /**
     * Constructor.
     *
     * @param readTimestamp Start timestamp of the transaction.
     * @param txId Transaction ID.
     */
    TxIdAndTimestamp(HybridTimestamp readTimestamp, UUID txId) {
        this.readTimestamp = readTimestamp;
        this.txId = txId;
    }

    /**
     * Returns start timestamp of the transaction.
     */
    public HybridTimestamp getReadTimestamp() {
        return readTimestamp;
    }

    /**
     * Returns the transaction ID.
     */
    public UUID getTxId() {
        return txId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TxIdAndTimestamp that = (TxIdAndTimestamp) o;

        return readTimestamp.equals(that.readTimestamp) && txId.equals(that.txId);
    }

    @Override
    public int hashCode() {
        int result = readTimestamp.hashCode();
        result = 31 * result + txId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return S.toString(TxIdAndTimestamp.class, this);
    }
}
