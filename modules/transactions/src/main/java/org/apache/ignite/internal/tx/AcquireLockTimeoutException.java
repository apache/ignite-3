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
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_TIMEOUT_ERR;

import org.apache.ignite.internal.tx.impl.VolatileTxStateMetaStorage;

/**
 * This exception is thrown when a lock cannot be acquired in a timeout boundaries.
 */
public class AcquireLockTimeoutException extends LockException {
    /**
     * Constructor.
     *
     * @param waiter Waiter for lock acquisition.
     * @param exceededTimeout Exceeded timeout value in milliseconds.
     * @param txStateVolatileStorage tx storage to retrieve label for logging.
     */
    public AcquireLockTimeoutException(Waiter waiter, long exceededTimeout, VolatileTxStateMetaStorage txStateVolatileStorage) {
        super(
                ACQUIRE_LOCK_TIMEOUT_ERR,
                "Failed to acquire a lock due to timeout ["
                        +  formatTxInfo(waiter.txId(), txStateVolatileStorage)
                        + ", waiter=" + waiter
                        + ", timeoutMs=" + exceededTimeout
                        + ']'
        );
    }
}
