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

import static org.apache.ignite.internal.util.retry.TimeoutState.attempt;
import static org.apache.ignite.internal.util.retry.TimeoutState.timeout;

public class CommonRetryContext {

    private final int initialTimeout;

    private final TimeoutStrategy timeoutStrategy;

    private final TimeoutState timeoutState;

    public CommonRetryContext(int initialTimeout, TimeoutStrategy timeoutStrategy) {
        this.initialTimeout = initialTimeout;
        this.timeoutStrategy = timeoutStrategy;

        timeoutState = new TimeoutState(initialTimeout, 1);
    }

    public TimeoutState getState() {
        return timeoutState;
    }

    public TimeoutState updateAndGetState() {
        long currentState;
        int next;
        do {
            currentState = timeoutState.getRawState();
            next = timeoutStrategy.next(timeout(currentState));
        } while (!timeoutState.update(currentState, next, attempt(currentState) + 1));

        return timeoutState;
    }

    public void resetState() {
        long currentState;
        do {
            currentState = timeoutState.getRawState();
        } while (!timeoutState.update(currentState, initialTimeout, 1));
    }
}
