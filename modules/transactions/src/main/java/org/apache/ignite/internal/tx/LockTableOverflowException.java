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

import static org.apache.ignite.internal.tx.TransactionLogUtils.formatTxInfo;
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_ERR;

import java.util.UUID;
import org.apache.ignite.internal.tx.impl.VolatileTxStateMetaStorage;

/**
 * This exception is thrown when the lock map size exceeded and new lock cannot be acquired and placed there.
 */
public class LockTableOverflowException extends LockException {
    /**
     * Constructor.
     *
     * @param failedTx Transaction that couldn't acquire a lock.
     * @param lockMapSizeLimit Current lock map size limit that was exceeded.
     * @param txStateVolatileStorage tx storage to retrieve label for logging.
     */
    public LockTableOverflowException(UUID failedTx, int lockMapSizeLimit, VolatileTxStateMetaStorage txStateVolatileStorage) {
        super(
                ACQUIRE_LOCK_ERR,
                "Failed to acquire a lock due to lock table overflow ["
                        +  formatTxInfo(failedTx, txStateVolatileStorage)
                        + ", lockMapSizeLimit=" + lockMapSizeLimit + ']'
        );
    }
}
