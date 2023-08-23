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
    /** The flag distinguishes locally initiated transactions. */
    private static long LOCAL = 1L << 33;

    /**
     * Creates a transaction ID from the given begin timestamp and nodeId.
     *
     * @param beginTimestamp Transaction begin timestamp.
     * @param nodeId Unique ID of the current node used to make generated transaction IDs globally unique.
     * @return Transaction ID corresponding to the provided values.
     */
    public static UUID transactionId(HybridTimestamp beginTimestamp, int nodeId, boolean local) {
        long leastSigBits =  Integer.toUnsignedLong(nodeId) | (local ? LOCAL : 0L);

        return new UUID(beginTimestamp.longValue(), leastSigBits);
    }

    /**
     * Extracts the local flag from transaction ID.
     *
     * @param transactionId Transaction ID.
     * @return Local flag.
     */
    public static boolean isLocal(UUID transactionId) {
        return (transactionId.getLeastSignificantBits() & LOCAL) == LOCAL;
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
}
