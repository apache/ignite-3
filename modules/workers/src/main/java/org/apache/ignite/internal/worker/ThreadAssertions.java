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

import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.ThreadAttributes;
import org.apache.ignite.internal.thread.ThreadOperation;

/**
 * Tools to assert that the current thread allows to perform a requested operation.
 */
public class ThreadAssertions {
    public static final String ENABLED_PROPERTY = "ignite.thread.assertions.enabled";

    private static final IgniteLogger LOG = Loggers.forClass(ThreadAssertions.class);

    private static final boolean ENABLED = Boolean.parseBoolean(System.getProperty(ENABLED_PROPERTY, "true"));

    /**
     * Returns {@code true} if thread assertions are enabled.
     */
    public static boolean enabled() {
        return ENABLED;
    }

    /**
     * Assert that the current thread allows to perform {@link ThreadOperation#STORAGE_WRITE} operations.
     */
    public static void assertThreadAllowsToWrite() {
        assertThreadAllowsTo(ThreadOperation.STORAGE_WRITE);
    }

    /**
     * Assert that the current thread allows to perform {@link ThreadOperation#STORAGE_READ} operations.
     */
    public static void assertThreadAllowsToRead() {
        assertThreadAllowsTo(ThreadOperation.STORAGE_READ);
    }

    private static void assertThreadAllowsTo(ThreadOperation requestedOperation) {
        Thread currentThread = Thread.currentThread();

        // TODO: IGNITE-21439 - actually throw AssertionError if the operation is not allowed.

        if (!(currentThread instanceof ThreadAttributes)) {
            LOG.warn("Thread {} does not have allowed operations", trackerException(), currentThread);

            return;
        }

        if (!((ThreadAttributes) currentThread).allows(requestedOperation)) {
            LOG.warn("Thread {} is not allowed to {}", trackerException(), currentThread, requestedOperation);
        }
    }

    private static Exception trackerException() {
        return new Exception("Tracker");
    }
}
