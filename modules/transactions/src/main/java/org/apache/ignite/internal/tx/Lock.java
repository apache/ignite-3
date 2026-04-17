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

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.tostring.S;

/** Lock. */
public class Lock {
    /** Lock key. */
    private final LockKey lockKey;

    /** Lock mode. */
    private final LockMode lockMode;

    /** Transaction identificator. */
    private final UUID txId;

    /**
     * The constructor.
     *
     * @param lockKey Lock key.
     * @param lockMode Lock mode.
     * @param txId Transaction id.
     */
    public Lock(LockKey lockKey, LockMode lockMode, UUID txId) {
        this.lockKey = lockKey;
        this.lockMode = lockMode;
        this.txId = txId;
    }

    /**
     * Returns lock key.
     *
     * @return Lock key.
     */
    public LockKey lockKey() {
        return lockKey;
    }

    /**
     * Returns lock mode.
     *
     * @return Lock mode.
     */
    public LockMode lockMode() {
        return lockMode;
    }

    /**
     * Returns transcation identificator.
     *
     * @return Tx id.
     */
    public UUID txId() {
        return txId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Lock lock = (Lock) o;
        return Objects.equals(lockKey, lock.lockKey) && lockMode == lock.lockMode && Objects.equals(txId, lock.txId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lockKey, lockMode, txId);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
