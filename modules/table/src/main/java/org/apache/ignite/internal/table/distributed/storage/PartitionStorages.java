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

package org.apache.ignite.internal.table.distributed.storage;

import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;

/**
 * Partition storages holder.
 */
public class PartitionStorages {
    private final MvPartitionStorage mvPartitionStorage;

    private final TxStateStorage txStateStorage;

    /**
     * Constructor.
     *
     * @param mvPartitionStorage Multi-versioned storage.
     * @param txStateStorage Transaction state storage.
     */
    public PartitionStorages(MvPartitionStorage mvPartitionStorage, TxStateStorage txStateStorage) {
        this.mvPartitionStorage = mvPartitionStorage;
        this.txStateStorage = txStateStorage;
    }

    /**
     * Returns multi-versioned storage.
     */
    public MvPartitionStorage getMvPartitionStorage() {
        return mvPartitionStorage;
    }

    /**
     * Returns transaction state storage.
     */
    public TxStateStorage getTxStateStorage() {
        return txStateStorage;
    }
}
