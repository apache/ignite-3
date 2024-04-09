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

package org.apache.ignite.internal.worker;

import static org.apache.ignite.internal.lang.IgniteSystemProperties.THREAD_ASSERTIONS_ENABLED;

import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.thread.ThreadAttributes;
import org.apache.ignite.internal.thread.ThreadOperation;

/**
 * Tools to assert that the current thread allows to perform a requested operation.
 */
public class ThreadAssertions {
    private static final IgniteLogger LOG = Loggers.forClass(ThreadAssertions.class);

    /**
     * Returns {@code true} if thread assertions are enabled.
     */
    public static boolean enabled() {
        return IgniteSystemProperties.getBoolean(THREAD_ASSERTIONS_ENABLED, false);
    }

    /**
     * Asserts that the current thread allows to perform {@link ThreadOperation#STORAGE_WRITE} operations.
     */
    public static void assertThreadAllowsToWrite() {
        assertThreadAllowsTo(ThreadOperation.STORAGE_WRITE);
    }

    /**
     * Asserts that the current thread allows to perform {@link ThreadOperation#STORAGE_READ} operations.
     */
    public static void assertThreadAllowsToRead() {
        assertThreadAllowsTo(ThreadOperation.STORAGE_READ);
    }

    /**
     * Asserts that the current thread allows to perform the requested operation.
     */
    public static void assertThreadAllowsTo(ThreadOperation requestedOperation) {
        Thread currentThread = Thread.currentThread();

        if (!(currentThread instanceof ThreadAttributes)) {
            if (PublicApiThreading.executingSyncPublicApi()) {
                // Allow everything if we ride a user thread while executing a public API call.

                return;
            }

            AssertionError error = new AssertionError("Thread " + currentThread.getName() + " does not have allowed operations");

            if (logBeforeThrowing()) {
                LOG.warn("Thread {} does not have allowed operations", error, currentThread);
            }

            throw error;
        }

        if (!((ThreadAttributes) currentThread).allows(requestedOperation)) {
            AssertionError error = new AssertionError("Thread " + currentThread.getName() + " is not allowed to do " + requestedOperation);

            if (logBeforeThrowing()) {
                LOG.warn("Thread {} is not allowed to do {}", error, currentThread, requestedOperation);
            }

            throw error;
        }
    }

    private static boolean logBeforeThrowing() {
        return IgniteSystemProperties.getBoolean(IgniteSystemProperties.THREAD_ASSERTIONS_LOG_BEFORE_THROWING, true);
    }
}
