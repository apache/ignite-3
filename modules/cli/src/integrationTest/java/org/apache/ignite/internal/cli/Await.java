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

package org.apache.ignite.internal.cli;

import java.time.Duration;
import java.util.function.Supplier;

/** Utility class to await some condition. */
public final class Await {

    private Await() {
    }

    /** Awaits a condition. */
    public static void await(Supplier<Boolean> supplier, Duration timeout) {
        long startTime = System.currentTimeMillis();
        long currentTime = startTime;
        while (currentTime - startTime < timeout.toMillis()) {
            if (supplier.get()) {
                return;
            } else {
                currentTime = System.currentTimeMillis();
            }
        }
        throw new AwaitException("Timeout exceed");
    }

    /** AwaitException. */
    public static class AwaitException extends RuntimeException {
        AwaitException(String message) {
            super(message);
        }
    }
}
