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

package org.apache.ignite.internal.sql.api;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.sql.engine.CurrentTimeProvider;

/**
 * A tracker to check if particular object has not been accessed longer than specified duration.
 *
 * <p>There are two different types of interaction:<ol>
 *     <li>
 *         `touch()` -- updates last access time. Use this method whenever access to the object
 *         is considered legit, thus should prevent it from expiration.
 *     </li>
 *     <li>
 *         `expired()` -- only checks if object wasn't been accessed longer than specified timeout,
 *         but doesn't update access time.
 *     </li>
 * </ol>
 *
 */
class IdleExpirationTracker {
    /** Marker used to mark a session which has been expired. */
    private static final long EXPIRED = 0L;

    private final long idleTimeoutMs;
    private final AtomicLong lastTouched;
    private final CurrentTimeProvider currentTimeProvider;

    IdleExpirationTracker(
            long idleTimeoutMs,
            CurrentTimeProvider currentTimeProvider
    ) {
        this.idleTimeoutMs = idleTimeoutMs;
        this.currentTimeProvider = currentTimeProvider;

        lastTouched = new AtomicLong(currentTimeProvider.now());
    }

    /**
     * Checks if the given object has not been accessed longer than specified duration.
     *
     * @return {@code true} if object is expired.
     */
    @SuppressWarnings("SimplifiableIfStatement")
    boolean expired() {
        long last = lastTouched.get();

        if (last == EXPIRED) {
            return true;
        }

        return currentTimeProvider.now() - last > idleTimeoutMs
                && (lastTouched.compareAndSet(last, EXPIRED) || lastTouched.get() == EXPIRED);
    }

    /**
     * Updates the timestamp that is used to determine whether the object has expired or not.
     *
     * @return {@code true} if this object has been updated, otherwise returns {@code false}
     *      meaning the object has expired.
     */
    boolean touch() {
        long last;
        long now;
        do {
            last = lastTouched.get();

            // tracker has been marked as expired
            if (last == EXPIRED) {
                return false;
            }

            now = currentTimeProvider.now();

            if (now - last > idleTimeoutMs && expired()) {
                return false;
            }
        } while (!lastTouched.compareAndSet(last, now));

        return true;
    }
}
