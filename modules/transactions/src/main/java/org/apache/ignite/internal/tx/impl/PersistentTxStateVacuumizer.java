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
import java.util.function.Function;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;

public class PersistentTxStateVacuumizer {
    private final Function<TablePartitionId, TxStateStorage> txStateStorageResolver;

    public PersistentTxStateVacuumizer(Function<TablePartitionId, TxStateStorage> txStateStorageResolver) {
        this.txStateStorageResolver = txStateStorageResolver;
    }

    public void vacuumPersistentTxState(UUID txId, TablePartitionId commitPartitionId) {
        TxStateStorage txStateStorage = txStateStorageResolver.apply(commitPartitionId);

        if (txStateStorage != null) {
            txStateStorage.remove(txId);
        }
    }
}
