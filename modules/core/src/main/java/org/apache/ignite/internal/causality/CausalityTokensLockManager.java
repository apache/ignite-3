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
package org.apache.ignite.internal.causality;

/**
 * Lock manager that manages locks on causality tokens between multiple {@code VersionedValues}.
 * When token is locked using some {@link VersionedValue} (see {@link VersionedValue#lock(long)}),
 * it will be locked on every {@link VersionedValue} that shares the same {@link CausalityTokensLockManager}.
 */
// TODO https://issues.apache.org/jira/browse/IGNITE-16544
public class CausalityTokensLockManager {
    /**
     * Acquire read lock on given token to prevent its deletion from history of every {@link VersionedValue}
     * managed by this manager.
     *
     * @param causalityToken Causality token.
     * @throws OutdatedTokenException If outdated token is passed as an argument. See also {@link #tryWriteLock(long, VersionedValue)}.
     */
    public void readLock(long causalityToken) throws OutdatedTokenException {

    }

    /**
     * Tries to acquire write lock on given token to allow its deletion from history of given {@link VersionedValue}
     * that supposed to be managed by this manager.<br>
     * Once this method has successfully acquired the lock on some token, it means that this token is being deleted from
     * history and {@link #readLock(long)} will always throw {@link OutdatedTokenException} on every call with this token.
     *
     * @param causalityToken Causality token.
     * @throws OutdatedTokenException If outdated token is passed as an argument.
     */
    public boolean tryWriteLock(long causalityToken, VersionedValue<?> versionedValue) throws OutdatedTokenException {
        return false;
    }

    /**
     * Unlocks the token, previously locked by {@link #readLock(long)}.
     *
     * @param causalityToken Causality token.
     */
    public void readUnlock(long causalityToken) {

    }

    /**
     * Unlocks the token, previously locked by {@link #tryWriteLock(long, VersionedValue)}.
     *
     * @param causalityToken Causality token.
     */
    public void writeUnlock(long causalityToken, VersionedValue<?> versionedValue) {

    }
}
