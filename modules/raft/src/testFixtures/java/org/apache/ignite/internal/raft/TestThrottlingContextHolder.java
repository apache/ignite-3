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

package org.apache.ignite.internal.raft;

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Implementation of {@link ThrottlingContextHolder} for testing purposes.
 */
public class TestThrottlingContextHolder implements ThrottlingContextHolder {
    private final long peerRequestTimeoutMillis;

    /**
     * Constructor.
     *
     * @param peerRequestTimeoutMillis Peer request timeout in milliseconds.
     */
    private TestThrottlingContextHolder(long peerRequestTimeoutMillis) {
        this.peerRequestTimeoutMillis = peerRequestTimeoutMillis;
    }

    /**
     * Creates a new instance of {@link ThrottlingContextHolder} for testing purposes with a default timeout of 3000 ms.
     *
     * @return A new instance of {@link ThrottlingContextHolder} configured for testing.
     */
    @TestOnly
    public static ThrottlingContextHolder throttlingContextHolder() {
        return testThrottlingContext(3000L);
    }

    /**
     * Creates a new instance of {@link ThrottlingContextHolder} for testing purposes with a specified timeout.
     *
     * @param peerRequestTimeoutMillis Peer request timeout in milliseconds.
     * @return A new instance of {@link ThrottlingContextHolder} configured for testing with the specified timeout.
     */
    @TestOnly
    public static ThrottlingContextHolder testThrottlingContext(long peerRequestTimeoutMillis) {
        return new TestThrottlingContextHolder(peerRequestTimeoutMillis);
    }

    @Override
    public boolean isOverloaded() {
        return false;
    }

    @Override
    public void beforeRequest() {
        // No-op.
    }

    @Override
    public void afterRequest(long requestStartTimestamp, @Nullable Boolean retriableError) {
        // No-op.
    }

    @Override
    public long peerRequestTimeoutMillis() {
        return peerRequestTimeoutMillis;
    }

    @Override
    public ThrottlingContextHolder peerContextHolder(String consistentId) {
        return this;
    }

    @Override
    public void onNodeLeft(String consistentId) {
        // No-op.
    }
}
