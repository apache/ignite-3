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

import java.util.Set;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/**
 * Partition enlistment information used when finishing a transaction (after its commit timestamp is chosen) and when doing its cleanup.
 */
public class PartitionEnlistment {
    private final String primaryNodeConsistentId;

    @IgniteToStringInclude
    protected final Set<Integer> tableIds;

    public PartitionEnlistment(String primaryNodeConsistentId, Set<Integer> tableIds) {
        this.primaryNodeConsistentId = primaryNodeConsistentId;
        this.tableIds = tableIds;
    }

    /**
     * Returns consistent ID of the primary node.
     */
    public String primaryNodeConsistentId() {
        return primaryNodeConsistentId;
    }

    /**
     * Returns IDs of tables for which the partition is enlisted.
     */
    public Set<Integer> tableIds() {
        return tableIds;
    }

    @Override
    public String toString() {
        return S.toString(PartitionEnlistment.class, this);
    }
}
