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

package org.apache.ignite.internal.tx.event;

import java.util.UUID;
import org.apache.ignite.internal.event.EventParameters;
import org.apache.ignite.internal.tx.LockKey;

/**
 * The class presents parameters that could be interesting to handle with an event.
 */
public class LockEventParameters extends EventParameters {
    /** Locked key. */
    private final LockKey lockKey;

    /** Transaction ID, which holds a lock on the key. */
    private final UUID lockHolderTx;

    /** Transaction ID, which cannot take a lock due to the conflict with the lock holder transaction. */
    private final UUID lockAcquirerTx;

    /**
     * The constructor.
     *
     * @param lockKey Lock key.
     * @param lockHolderTx ID of transaction holding the lock.
     * @param lockAcquirerTx ID of conflicted transaction by the lock.
     */
    public LockEventParameters(LockKey lockKey, UUID lockHolderTx, UUID lockAcquirerTx) {
        super(0);

        this.lockKey = lockKey;
        this.lockHolderTx = lockHolderTx;
        this.lockAcquirerTx = lockAcquirerTx;
    }

    /**
     * Gets lock key.
     *
     * @return Lock key.
     */
    public LockKey getLockKey() {
        return lockKey;
    }

    /**
     * Gets a lock holder transaction ID.
     *
     * @return Transaction ID.
     */
    public UUID getLockHolderTx() {
        return lockHolderTx;
    }

    /**
     * Gets a lock acquirer transaction ID.
     *
     * @return Transaction ID.
     */
    public UUID getLockAcquirerTx() {
        return lockAcquirerTx;
    }
}
