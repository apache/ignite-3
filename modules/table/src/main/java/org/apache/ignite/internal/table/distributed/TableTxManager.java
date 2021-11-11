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

package org.apache.ignite.internal.table.distributed;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.network.ClusterService;

/**
 * Distributed transaction manager.
 * <p>Used to support transactions across tables.
 */
public class TableTxManager extends TxManagerImpl {
    private final Loza raftManager;
    
    /**
     * @param clusterService Cluster service.
     * @param lockManager Lock manager.
     * @param raftManager Raft manager.
     */
    public TableTxManager(ClusterService clusterService, LockManager lockManager, Loza raftManager) {
        super(clusterService, lockManager);
        this.raftManager = raftManager;
    }
    
    /** {@inheritDoc} */
    @Override
    protected CompletableFuture<?> onFinish(String groupId, Timestamp ts, boolean commit) {
        return raftManager.apply(groupId, new FinishTxCommand(ts, commit));
    }
}
