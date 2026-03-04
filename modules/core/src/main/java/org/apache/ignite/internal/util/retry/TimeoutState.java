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

package org.apache.ignite.internal.util.retry;

import java.util.concurrent.atomic.AtomicLong;

public class TimeoutState {
    private final AtomicLong state = new AtomicLong();

    public TimeoutState(int initialTimeout, int initialAttempt) {
        state.set(pack(initialTimeout, initialAttempt));
    }

    public long getRawState() {
        return state.get();
    }

    public int getTimeout() {
        return timeout(state.get());
    }

    public int getAttempt() {
        return attempt(state.get());
    }

    public boolean update(long currentState, int newTimeout, int newAttempt) {
        return state.compareAndSet(currentState, pack(newTimeout, newAttempt));
    }

    private static long pack(int timeout, int attempt) {
        return ((long) timeout << 32) | (attempt & 0xFFFFFFFFL);
    }

    static int timeout(long packed) {
        return (int) (packed >>> 32);
    }

    static int attempt(long packed) {
        return (int) packed;
    }
}