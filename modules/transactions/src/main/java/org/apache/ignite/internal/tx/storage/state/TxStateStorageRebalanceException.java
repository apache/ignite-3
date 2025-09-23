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

package org.apache.ignite.internal.tx.storage.state;

import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_REBALANCE_ERR;

import org.jetbrains.annotations.Nullable;

/**
 * Exception that will be thrown when the storage is in the process of rebalance.
 */
public class TxStateStorageRebalanceException extends TxStateStorageException {
    private static final long serialVersionUID = -4840074471464728969L;

    /**
     * Default constructor.
     */
    public TxStateStorageRebalanceException() {
        this("Storage in the process of rebalancing");
    }

    /**
     * Constructor.
     *
     * @param message Error message.
     */
    public TxStateStorageRebalanceException(String message) {
        super(TX_STATE_STORAGE_REBALANCE_ERR, message);
    }

    /**
     * Constructor.
     *
     * @param message Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public TxStateStorageRebalanceException(String message, @Nullable Throwable cause) {
        super(TX_STATE_STORAGE_REBALANCE_ERR, message, cause);
    }
}
