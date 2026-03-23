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

package org.apache.ignite.internal.schema;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.testframework.TestIgnitionManager;

/**
 * Allows to make all subsequent schema sync requests wait indefinitely in tests.
 */
public class SchemaSyncInhibitor {
    private final SchemaSafeTimeTrackerImpl schemaSafeTimeTracker;

    private final CompletableFuture<Void> inhibitFuture = new CompletableFuture<>();

    /** Constructor. */
    public SchemaSyncInhibitor(Ignite ignite) {
        schemaSafeTimeTracker = unwrapIgniteImpl(ignite).schemaSafeTimeTracker();
    }

    /**
     * Starts inhibiting schema sync.
     */
    public void startInhibit() {
        schemaSafeTimeTracker.enqueueFuture(inhibitFuture);

        waitForSchemaSyncRequiringWait();
    }

    private static void waitForSchemaSyncRequiringWait() {
        try {
            Thread.sleep(TestIgnitionManager.DEFAULT_DELAY_DURATION_MS + 1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new RuntimeException(e);
        }
    }

    /**
     * Stops inhibiting schema sync.
     */
    public void stopInhibit() {
        inhibitFuture.complete(null);
    }

    /**
     * Executes an action enclosed in schema sync inhibition: that is, before execution inhibition gets started, and after the execution
     * it gets stopped.
     *
     * @param action Action to execute.
     * @return Action result.
     */
    public <T> T withInhibition(Supplier<? extends T> action) {
        startInhibit();

        try {
            return action.get();
        } finally {
            stopInhibit();
        }
    }

    /**
     * Executes an action enclosed in schema sync inhibition: that is, before execution inhibition gets started, and after the execution
     * it gets stopped.
     *
     * @param action Action to execute.
     */
    public void withInhibition(Runnable action) {
        startInhibit();

        try {
            action.run();
        } finally {
            stopInhibit();
        }
    }

    /**
     * Executes an action enclosed in schema sync inhibition: that is, before execution inhibition gets started, and after the execution
     * it gets stopped.
     *
     * @param ignite Node on which to inhibit schema sync.
     * @param action Action to execute.
     * @return Action result.
     */
    public static <T> T withInhibition(Ignite ignite, Supplier<? extends T> action) {
        return new SchemaSyncInhibitor(ignite).withInhibition(action);
    }

    /**
     * Executes an action enclosed in schema sync inhibition: that is, before execution inhibition gets started, and after the execution
     * it gets stopped.
     *
     * @param ignite Node on which to inhibit schema sync.
     * @param action Action to execute.
     */
    public static void withInhibition(Ignite ignite, Runnable action) {
        new SchemaSyncInhibitor(ignite).withInhibition(action);
    }
}
