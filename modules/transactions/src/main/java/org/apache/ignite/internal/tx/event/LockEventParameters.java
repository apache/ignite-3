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

/**
 * This class encapsulates event parameters related to lock management.
 */
public class LockEventParameters implements EventParameters {
    /** Transaction ID, which holds a lock on the key. */
    private final UUID lockHolderTx;

    /** Transaction ID, which cannot take a lock due to the conflict with the lock holder transaction. */
    private final UUID lockAcquirerTx;

    /**
     * The constructor.
     *
     * @param lockAcquirerTx ID of conflicted transaction by the lock.
     * @param lockHolderTx ID of transaction holding the lock.
     */
    public LockEventParameters(UUID lockAcquirerTx, UUID lockHolderTx) {
        this.lockHolderTx = lockHolderTx;
        this.lockAcquirerTx = lockAcquirerTx;
    }

    /**
     * Gets a lock holder transaction ID.
     *
     * @return Transaction ID.
     */
    public UUID lockHolderTx() {
        return lockHolderTx;
    }

    /**
     * Gets a lock acquirer transaction ID.
     *
     * @return Transaction ID.
     */
    public UUID lockAcquirerTx() {
        return lockAcquirerTx;
    }
}
