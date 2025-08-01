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

import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.tostring.S;

/**
 * Partition enlistment information in a pending transaction. It stores information needed before commit timestamp is generated.
 */
public class PendingTxPartitionEnlistment extends PartitionEnlistment {
    private final long consistencyToken;

    /**
     * Creates a new enlistment adding the given table ID to it.
     *
     * @param primaryNodeConsistentId Primary node consistent ID.
     * @param consistencyToken Enlistment consistency token.
     * @param tableId ID of the table.
     */
    public PendingTxPartitionEnlistment(String primaryNodeConsistentId, long consistencyToken, int tableId) {
        this(primaryNodeConsistentId, consistencyToken);

        tableIds.add(tableId);
    }

    /**
     * Creates a new enlistment with empty table ID set.
     *
     * @param primaryNodeConsistentId Primary node.
     * @param consistencyToken Enlistment consistency token.
     */
    public PendingTxPartitionEnlistment(String primaryNodeConsistentId, long consistencyToken) {
        super(primaryNodeConsistentId, ConcurrentHashMap.newKeySet());

        this.consistencyToken = consistencyToken;
    }

    /**
     * Adds a table ID to the enlistment (that is, marks this enlistment as touching the given table ID in the current partition).
     *
     * @param tableId Table ID to add.
     */
    public void addTableId(int tableId) {
        tableIds.add(tableId);
    }

    /**
     * Returns enlistment consistency token.
     */
    public long consistencyToken() {
        return consistencyToken;
    }

    @Override
    public String toString() {
        return S.toString(PendingTxPartitionEnlistment.class, this, super.toString());
    }
}
