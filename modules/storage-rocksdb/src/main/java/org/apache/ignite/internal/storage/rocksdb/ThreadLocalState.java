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

package org.apache.ignite.internal.storage.rocksdb;

import org.apache.ignite.internal.storage.MvPartitionStorage.Locker;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.util.LocalLocker;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.WriteBatchWithIndex;

/**
 * Thread-local state of the MV storage during the execution of {@link WriteClosure}.
 */
class ThreadLocalState {
    /** Write batch instance that will be written at the end of {@link RocksDbMvPartitionStorage#runConsistently(WriteClosure)}. */
    final WriteBatchWithIndex batch;

    /** Locker instance, used in {@link WriteClosure#execute(Locker)}. */
    final LocalLocker locker;

    long pendingAppliedIndex;
    long pendingAppliedTerm;
    byte @Nullable [] pendingGroupConfig;
    long pendingEstimatedSizeDiff;

    ThreadLocalState(WriteBatchWithIndex batch, LocalLocker locker) {
        this.batch = batch;
        this.locker = locker;
    }
}
