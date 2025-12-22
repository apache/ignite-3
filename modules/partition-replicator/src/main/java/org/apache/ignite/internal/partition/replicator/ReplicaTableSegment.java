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

package org.apache.ignite.internal.partition.replicator;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;

/**
 * Segment of a partition replica corresponding to a specific table.
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-27405 - remove this as we'll not need to pass safe time tracker.
public class ReplicaTableSegment {
    private final TableTxRwOperationTracker txRwOperationTracker;
    private final PendingComparableValuesTracker<HybridTimestamp, Void> safeTime;

    public ReplicaTableSegment(
            TableTxRwOperationTracker txRwOperationTracker,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTime
    ) {
        this.txRwOperationTracker = txRwOperationTracker;
        this.safeTime = safeTime;
    }

    public TableTxRwOperationTracker txRwOperationTracker() {
        return txRwOperationTracker;
    }

    public PendingComparableValuesTracker<HybridTimestamp, Void> safeTime() {
        return safeTime;
    }
}
