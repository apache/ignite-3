/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.pagememory.mv;

import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;

import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Utils to work with Transaction IDs.
 */
public class TransactionIds {
    /**
     * Writes transaction ID to memory starting at the specified address.
     *
     * @param addr   addreds where to start writing
     * @param offset offset to add to the address
     * @param txId   transaction ID to write
     * @return number of bytes written
     */
    public static int writeTransactionId(long addr, int offset, @Nullable UUID txId) {
        long txIdHigh;
        long txIdLow;
        if (txId != null) {
            txIdHigh = txId.getMostSignificantBits();
            txIdLow = txId.getLeastSignificantBits();
        } else {
            txIdHigh = VersionChain.NULL_UUID_COMPONENT;
            txIdLow = VersionChain.NULL_UUID_COMPONENT;
        }

        putLong(addr, offset, txIdHigh);
        putLong(addr, offset + Long.BYTES, txIdLow);

        return 2 * Long.BYTES;
    }

    private TransactionIds() {
        // prevent instantiation
    }
}
