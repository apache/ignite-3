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

package org.apache.ignite.internal.replicator;

import java.util.concurrent.Executor;
import org.apache.ignite.internal.thread.ExecutorChooser;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;

/**
 * Executor chooser that makes its choice based on {@link ReplicationGroupId} it is provided.
 */
public class ChooseExecutorForReplicationGroup implements ExecutorChooser<ReplicationGroupId> {
    private final StripedThreadPoolExecutor partitionOperationsPool;

    /** Constructor. */
    public ChooseExecutorForReplicationGroup(StripedThreadPoolExecutor partitionOperationsPool) {
        this.partitionOperationsPool = partitionOperationsPool;
    }

    @Override
    public Executor choose(ReplicationGroupId groupId) {
        return ReplicationGroupStripes.stripeFor(groupId, partitionOperationsPool);
    }
}
