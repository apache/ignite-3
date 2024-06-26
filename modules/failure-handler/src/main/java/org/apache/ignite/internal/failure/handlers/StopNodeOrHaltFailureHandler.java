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

package org.apache.ignite.internal.failure.handlers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.handlers.configuration.StopNodeOrHaltFailureHandlerView;
import org.apache.ignite.internal.tostring.S;

/**
 * Handler will try to stop node if {@code tryStop} value is {@code true}.
 * If node can't be stopped during provided {@code timeout} or {@code tryStop} value is {@code false}
 * then JVM process will be terminated forcibly using {@code Runtime.getRuntime().halt()}.
 */
public class StopNodeOrHaltFailureHandler extends AbstractFailureHandler {
    /**
     * This is kill code that can be used by external tools, like Shell scripts,
     * to auto-stop the Ignite JVM process without restarting.
     */
    private static final int KILL_EXIT_CODE = 130;

    /** Try stop. */
    private final boolean tryStop;

    /** Timeout in milliseconds. */
    private final long timeout;

    /**
     * Creates a new instance of a failure processor.
     */
    public StopNodeOrHaltFailureHandler() {
        this(false, 0);
    }

    /**
     * Creates a new instance of a failure processor.
     *
     * @param tryStop Try stop.
     * @param timeout Stop node timeout in milliseconds.
     */
    public StopNodeOrHaltFailureHandler(boolean tryStop, long timeout) {
        this.tryStop = tryStop;
        this.timeout = timeout;
    }

    /**
     * Creates a new instance of a failure processor.
     *
     * @param view Configuration view.
     */
    public StopNodeOrHaltFailureHandler(StopNodeOrHaltFailureHandlerView view) {
        tryStop = view.tryStop();
        timeout = view.timeoutMillis();
    }

    @Override
    protected boolean handle(String nodeName, FailureContext failureCtx) {
        if (tryStop) {
            CountDownLatch latch = new CountDownLatch(1);

            new Thread(
                    () -> {
                        IgnitionManager.stop(nodeName);

                        latch.countDown();
                    },
                    "node-stopper"
            ).start();

            new Thread(
                    () -> {
                        try {
                            if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
                                Runtime.getRuntime().halt(KILL_EXIT_CODE);
                            }
                        } catch (InterruptedException e) {
                            // No-op.
                        }
                    },
                    "jvm-halt-on-stop-timeout"
            ).start();
        } else {
            Runtime.getRuntime().halt(KILL_EXIT_CODE);
        }

        return true;
    }

    /**
     * Returns stop node timeout in milliseconds.
     *
     * @return Stop node timeout.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * Returns {@code true} if this handler should try to stop the node
     * before terminating JVM process using {@code Runtime.getRuntime().halt()}.
     *
     * @return Try stop.
     */
    public boolean tryStop() {
        return tryStop;
    }

    @Override public String toString() {
        return S.toString(StopNodeOrHaltFailureHandler.class, this);
    }
}
