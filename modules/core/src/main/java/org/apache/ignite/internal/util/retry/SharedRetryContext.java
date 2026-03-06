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

import static java.util.Optional.ofNullable;
import static org.apache.ignite.internal.util.retry.TimeoutState.attempt;
import static org.apache.ignite.internal.util.retry.TimeoutState.timeout;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class SharedRetryContext {

    private final int initialTimeout;

    private final TimeoutStrategy timeoutStrategy;

    private final AtomicReference<TimeoutState> timeoutState = new AtomicReference<>();

    public SharedRetryContext(int initialTimeout, TimeoutStrategy timeoutStrategy) {
        this.initialTimeout = initialTimeout;
        this.timeoutStrategy = timeoutStrategy;
    }

    public Optional<TimeoutState> getState() {
        return ofNullable(timeoutState.get());
    }

    public TimeoutState updateAndGetState() {
        while (true) {
            if (timeoutState.get() == null) {
                timeoutState.compareAndSet(null, new TimeoutState(initialTimeout, 0));
            }

            TimeoutState state = timeoutState.get();
            if (state == null) {
                continue; // reset raced us, retry
            }

            long raw;
            int nextTimeout;
            do {
                raw = state.getRawState();
                nextTimeout = attempt(raw) == 0
                        ? initialTimeout
                        : timeoutStrategy.next(timeout(raw));
            } while (!state.update(raw, nextTimeout, attempt(raw) + 1));

            return state;
        }
    }

    public void resetState() {
        timeoutState.set(null);
    }
}
