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

package org.apache.ignite.internal.failure;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.failure.handlers.AbstractFailureHandler;
import org.apache.ignite.internal.failure.handlers.FailureHandler;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.failure.handlers.StopNodeFailureHandler;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * General failure processing API.
 */
public class FailureProcessor implements IgniteComponent {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(FailureProcessor.class);

    /** Failure log message. */
    private static final String FAILURE_LOG_MSG = "Critical system error detected. "
            + "Will be handled accordingly to configured handler [hnd={}, failureCtx={}]";

    /** Ignored failure log message. */
    private static final String IGNORED_FAILURE_LOG_MSG = "Possible failure suppressed accordingly to a configured handler "
            + "[hnd={}, failureCtx={}]";

    /** Handler. */
    private final FailureHandler handler;

    /** Node name. */
    private final String nodeName;

    /** Interceptor of fail handler. Main purpose to make testing easier. */
    private @Nullable FailureHandler interceptor;

    /** Failure context. */
    private volatile FailureContext failureCtx;

    /**
     * Creates a new instance of a failure processor.
     *
     * @param nodeName Node name.
     * @param handler Handler.
     */
    public FailureProcessor(String nodeName, FailureHandler handler) {
        this.nodeName = nodeName;
        this.handler = handler;
    }

    /**
     * Creates a new instance of a failure processor.
     * The {@link StopNodeFailureHandler} will be used as a handler.
     *
     * @param nodeName Node name.
     */
    public FailureProcessor(String nodeName) {
        this.nodeName = nodeName;
        // TODO https://issues.apache.org/jira/browse/IGNITE-21456
        this.handler = new NoOpFailureHandler();
    }

    @Override
    public CompletableFuture<Void> startAsync(ExecutorService startupExecutor) {
        // TODO https://issues.apache.org/jira/browse/IGNITE-20450
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ExecutorService stopExecutor) {
        return nullCompletedFuture();
    }

    /**
     * Returns failure context.
     *
     * @return Failure context.
     */
    public FailureContext failureContext() {
        return failureCtx;
    }

    /**
     * Processes failure accordingly to configured {@link FailureHandler}.
     *
     * @param failureCtx Failure context.
     * @return {@code True} If this very call led to Ignite node invalidation.
     */
    public boolean process(FailureContext failureCtx) {
        return process(failureCtx, handler);
    }

    /**
     * Processes failure accordingly to given failure handler.
     *
     * @param failureCtx Failure context.
     * @param handler Failure handler.
     * @return {@code True} If this very call led to Ignite node invalidation.
     */
    private synchronized boolean process(FailureContext failureCtx, FailureHandler handler) {
        assert failureCtx != null : "Failure context is not initialized.";
        assert handler != null : "Failure handler is not initialized.";

        if (interceptor != null) {
            interceptor.onFailure(nodeName, failureCtx);
        }

        // Node already terminating, no reason to process more errors.
        if (this.failureCtx != null) {
            return false;
        }

        if (failureTypeIgnored(failureCtx, handler)) {
            LOG.warn(IGNORED_FAILURE_LOG_MSG, failureCtx.error(), handler, failureCtx);
        } else {
            LOG.error(FAILURE_LOG_MSG, failureCtx.error(), handler, failureCtx);
        }

        boolean invalidated = handler.onFailure(nodeName, failureCtx);

        if (invalidated) {
            this.failureCtx = failureCtx;
        }

        return invalidated;
    }

    /**
     * Returns {@code true} if the given failure type is ignored by the given handler.
     *
     * @param failureCtx Failure context.
     * @param handler Handler.
     */
    private static boolean failureTypeIgnored(FailureContext failureCtx, FailureHandler handler) {
        return handler instanceof AbstractFailureHandler
                && ((AbstractFailureHandler) handler).ignoredFailureTypes().contains(failureCtx.type());
    }

    /**
     * Set FailHander interceptor to provide ability tщ test scenarios related to fail handler.
     *
     * @param interceptor Interceptor of fails.
     */
    @TestOnly
    public synchronized void setInterceptor(@Nullable FailureHandler interceptor) {
        this.interceptor = interceptor;
    }
}
