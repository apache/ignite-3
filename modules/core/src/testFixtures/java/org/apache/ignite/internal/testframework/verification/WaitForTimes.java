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

package org.apache.ignite.internal.testframework.verification;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;

import org.mockito.Mockito;
import org.mockito.internal.verification.api.VerificationData;
import org.mockito.invocation.MatchableInvocation;
import org.mockito.verification.VerificationMode;

/**
 * Returns a verification mode to use with {@link Mockito#verify(Object, VerificationMode)} that waits for the wanted number of
 * invocations for the given timeout.
 */
public class WaitForTimes implements VerificationMode {
    private final long wantedTimes;
    private final long timeoutMillis;

    private WaitForTimes(long wantedTimes, long timeoutMillis) {
        this.wantedTimes = wantedTimes;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void verify(VerificationData data) {
        try {
            waitForCondition(() -> {
                MatchableInvocation wanted = data.getTarget();
                long actual = data.getAllInvocations().stream().filter(wanted::matches).count();

                if (actual > wantedTimes) {
                    throw new AssertionError("Called more times than expected: " + actual);
                }

                return actual == wantedTimes;
            }, timeoutMillis);
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Returns a verification mode to use with {@link Mockito#verify(Object, VerificationMode)} that waits for the wanted number of
     * invocations for the given timeout.
     *
     * @param wantedTimes Wanted times.
     * @param timeoutMillis Timeout, in milliseconds.
     * @return Verification mode.
     */
    public static VerificationMode waitForTimes(long wantedTimes, long timeoutMillis) {
        return new WaitForTimes(wantedTimes, timeoutMillis);
    }
}
