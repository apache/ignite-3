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

/**
 * A {@link TimeoutStrategy} that returns the current timeout unchanged on every call.
 *
 * <p>Useful when retry backoff is not desired — for example, in tests or when a flat
 * retry interval is intentional. The timeout passed to {@link #next(int)} is returned
 * as-is, so the retry interval remains constant across all attempts.
 *
 * <p>This class is stateless and thread-safe.
 */
public class NoopTimeoutStrategy implements TimeoutStrategy {
    /**
     * Returns {@code currentTimeout} unchanged.
     *
     * @param currentTimeout current retry timeout in milliseconds.
     * @return the same {@code currentTimeout} value, unmodified.
     */
    @Override
    public int next(int currentTimeout) {
        return currentTimeout;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int maxTimeout() {
        return DEFAULT_TIMEOUT_MS_MAX;
    }
}
