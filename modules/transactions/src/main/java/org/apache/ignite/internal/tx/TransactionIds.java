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

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;

/**
 * Collection of utils to generate and pick apart transaction IDs.
 */
public class TransactionIds {
    /**
     * Creates a transaction ID from the given begin timestamp and nodeId.
     *
     * @param beginTimestamp Transaction begin timestamp.
     * @param nodeId Unique ID of the current node used to make generated transaction IDs globally unique.
     * @param priority Transaction priority.
     * @return Transaction ID corresponding to the provided values.
     */
    public static UUID transactionId(HybridTimestamp beginTimestamp, int nodeId, TxPriority priority) {
        return transactionId(beginTimestamp.longValue(), nodeId, priority);
    }

    /**
     * Creates a transaction ID from the given begin timestamp and nodeId.
     *
     * @param beginTimestamp Transaction begin timestamp.
     * @param nodeId Unique ID of the current node used to make generated transaction IDs globally unique.
     * @return Transaction ID corresponding to the provided values.
     */
    public static UUID transactionId(HybridTimestamp beginTimestamp, int nodeId) {
        return transactionId(beginTimestamp.longValue(), nodeId, TxPriority.NORMAL);
    }

    /**
     * Creates a transaction ID from the given begin timestamp and nodeId.
     *
     * @param beginTimestamp Transaction begin timestamp.
     * @param nodeId Unique ID of the current node used to make generated transaction IDs globally unique.
     * @param priority Transaction priority.
     * @return Transaction ID corresponding to the provided values.
     */
    public static UUID transactionId(long beginTimestamp, int nodeId, TxPriority priority) {
        return new UUID(beginTimestamp, combine(nodeId, priority));
    }

    /**
     * Extracts begin timestamp from the provided transaction ID.
     *
     * @param transactionId Transaction ID.
     * @return Begin timestamp of the transaction.
     */
    public static HybridTimestamp beginTimestamp(UUID transactionId) {
        return hybridTimestamp(transactionId.getMostSignificantBits());
    }

    public static int nodeId(UUID transactionId) {
        return (int) (transactionId.getLeastSignificantBits() >> 32);
    }

    public static TxPriority priority(UUID txId) {
        int ordinal = (int) (txId.getLeastSignificantBits() & 1);
        return TxPriority.fromOrdinal(ordinal);
    }

    private static long combine(int nodeId, TxPriority priority) {
        int priorityAsInt = priority.ordinal();

        // Shift the int 32 bits and combine with the boolean
        return ((long) nodeId << 32) | priorityAsInt;
    }
}
