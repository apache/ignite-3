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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCauseOrSuppressed;
import static org.apache.ignite.lang.ErrorGroups.Common.COMPONENT_NOT_STARTED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.failure.configuration.FailureProcessorConfiguration;
import org.apache.ignite.internal.failure.configuration.FailureProcessorView;
import org.apache.ignite.internal.failure.handlers.AbstractFailureHandler;
import org.apache.ignite.internal.failure.handlers.FailureHandler;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.failure.handlers.StopNodeFailureHandler;
import org.apache.ignite.internal.failure.handlers.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.failure.handlers.configuration.FailureHandlerView;
import org.apache.ignite.internal.failure.handlers.configuration.NoOpFailureHandlerConfigurationSchema;
import org.apache.ignite.internal.failure.handlers.configuration.StopNodeFailureHandlerConfigurationSchema;
import org.apache.ignite.internal.failure.handlers.configuration.StopNodeOrHaltFailureHandlerConfigurationSchema;
import org.apache.ignite.internal.failure.handlers.configuration.StopNodeOrHaltFailureHandlerView;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.thread.ThreadUtils;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * General failure processing implementation.
 */
public class FailureManager implements FailureProcessor, IgniteComponent {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(FailureManager.class);

    /** Failure log message. */
    private static final String FAILURE_LOG_MSG = "Critical system error detected. "
            + "Will be handled accordingly to configured handler [hnd={}, failureCtx={}, failureCtxId={}]";

    /** Ignored failure log message. */
    private static final String IGNORED_FAILURE_LOG_MSG = "Possible failure suppressed according to a configured handler "
            + "[hnd={}, failureCtx={}, failureCtxId={}]";

    /** Ignite node name. */
    private final String nodeName;

    /** Failure processor configuration. */
    private final FailureProcessorConfiguration configuration;

    /** Handler. */
    private volatile FailureHandler handler;

    /** Node stopper. */
    private final NodeStopper nodeStopper;

    /** Interceptor of fail handler. Main purpose to make testing easier. */
    private @Nullable FailureHandler interceptor;

    /** Failure context. */
    private volatile FailureContext failureCtx;

    /** Reserve buffer, which can be dropped to handle {@link OutOfMemoryError}. */
    private volatile byte @Nullable [] reserveBuf;

    /** If this flag is true, then the failure processor prints threads dump. */
    private volatile boolean dumpThreadsOnFailure;

    /** Timeout for throttling of thread dumps generation (millis). */
    private volatile long dumpThreadsThrottlingTimeout;

    /** Thread dump per failure type timestamps. */
    private volatile @Nullable Map<FailureType, Long> threadDumpPerFailureTypeTs;

    /** Failure handler configuration listener. */
    private final FailureHandlerConfigurationListener configurationListener = new FailureHandlerConfigurationListener();

    /**
     * Creates a new instance of a failure processor.
     *
     * @param handler Handler.
     */
    @TestOnly
    public FailureManager(FailureHandler handler) {
        this.nodeName = "test-node";
        this.nodeStopper = () -> {};
        this.handler = handler;
        this.configuration = null;
    }

    /**
     * Creates a new instance of a failure processor.
     *
     * @param nodeName Node name.
     * @param nodeStopper Node stopper.
     * @param configuration Failure processor configuration.
     */
    public FailureManager(String nodeName, NodeStopper nodeStopper, FailureProcessorConfiguration configuration) {
        this.nodeName = nodeName;
        this.nodeStopper = nodeStopper;
        this.configuration = configuration;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        initFailureHandler();

        if (configuration != null) {
            configuration.listen(configurationListener);
        }

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (configuration != null) {
            configuration.stopListen(configurationListener);
        }

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
     * Returns {@code true} if the failure processor prints threads dump on failure.
     *
     * @return {@code true} if the failure processor prints threads dump on failure.
     **/
    public boolean dumpThreadsOnFailure() {
        return dumpThreadsOnFailure;
    }

    /**
     * Returns timeout for throttling of thread dumps generation (millis).
     *
     * @return Timeout for throttling of thread dumps generation (millis).
     **/
    public long dumpThreadsThrottlingTimeout() {
        return dumpThreadsThrottlingTimeout;
    }

    @Override
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
            interceptor.onFailure(failureCtx);
        }

        // Node already terminating, no reason to process more errors.
        if (this.failureCtx != null) {
            return false;
        }

        var exceptionForLogging = new StackTraceCapturingException(failureCtx.message(), failureCtx.error());
        if (handler.ignoredFailureTypes().contains(failureCtx.type())) {
            LOG.warn(IGNORED_FAILURE_LOG_MSG, exceptionForLogging, handler, failureCtx.type(), failureCtx.id());
        } else {
            LOG.error(FAILURE_LOG_MSG, exceptionForLogging, handler, failureCtx.type(), failureCtx.id());
        }

        if (reserveBuf != null && failureCtx.error() != null && hasCauseOrSuppressed(failureCtx.error(), OutOfMemoryError.class)) {
            reserveBuf = null;
        }

        if (dumpThreadsOnFailure && !throttleThreadDump(failureCtx)) {
            String ctxIdMsg = format(" [failureCtxId={}]", failureCtx.id());

            ThreadUtils.dumpThreads(LOG, ctxIdMsg, !handler.ignoredFailureTypes().contains(failureCtx.type()));
        }

        boolean invalidated = handler.onFailure(failureCtx);

        if (invalidated) {
            this.failureCtx = failureCtx;

            LOG.error("Ignite node is in invalid state due to a critical failure.");
        }

        return invalidated;
    }

    private void initFailureHandler() {
        if (configuration == null) {
            assert this.handler != null : "Failure handler is not initialized.";

            return;
        }

        reconfigure(configuration.value());
    }

    /**
     * Reconfigure failure processor.
     *
     * @param newConfiguration Configuration to be applied.
     */
    private synchronized void reconfigure(FailureProcessorView newConfiguration) {
        dumpThreadsOnFailure = newConfiguration.dumpThreadsOnFailure();
        dumpThreadsThrottlingTimeout = newConfiguration.dumpThreadsThrottlingTimeoutMillis();
        threadDumpPerFailureTypeTs = null;

        if (dumpThreadsOnFailure) {
            if (dumpThreadsThrottlingTimeout > 0) {
                Map<FailureType, Long> dumpPerFailureTypeTs = new EnumMap<>(FailureType.class);

                for (FailureType type : FailureType.values()) {
                    dumpPerFailureTypeTs.put(type, 0L);
                }

                threadDumpPerFailureTypeTs = dumpPerFailureTypeTs;
            }
        }

        if (reserveBuf == null || reserveBuf.length != newConfiguration.oomBufferSizeBytes()) {
            reserveBuf = new byte[newConfiguration.oomBufferSizeBytes()];
        }

        AbstractFailureHandler hnd;

        FailureHandlerView handlerView = newConfiguration.handler();

        switch (handlerView.type()) {
            case NoOpFailureHandlerConfigurationSchema.TYPE:
                hnd = new NoOpFailureHandler();
                break;

            case StopNodeFailureHandlerConfigurationSchema.TYPE:
                hnd = new StopNodeFailureHandler(nodeName, nodeStopper);
                if (true) {
                    throw new IgniteInternalException(INTERNAL_ERR, ">>>>> Unexpected failure handler!");
                }
                break;

            case StopNodeOrHaltFailureHandlerConfigurationSchema.TYPE:
                hnd = new StopNodeOrHaltFailureHandler(nodeName, nodeStopper, (StopNodeOrHaltFailureHandlerView) handlerView);
                break;

            default:
                throw new IgniteException(
                        COMPONENT_NOT_STARTED_ERR,
                        "Unknown failure handler type: " + handlerView.type());
        }

        String[] ignoredFailureTypes = handlerView.ignoredFailureTypes();

        Set<FailureType> ignoredFailureTypesSet = EnumSet.noneOf(FailureType.class);
        for (String ignoredFailureType : ignoredFailureTypes) {
            for (FailureType type : FailureType.values()) {
                if (type.typeName().equals(ignoredFailureType)) {
                    ignoredFailureTypesSet.add(type);
                }
            }
        }

        hnd.ignoredFailureTypes(ignoredFailureTypesSet);

        handler = hnd;

        LOG.info("Configured failure handler: [hnd={}]", handler);
    }

    /**
     * Set FailureHandler interceptor to provide ability to test scenarios related to failure handler.
     *
     * @param interceptor Interceptor of fails.
     */
    @TestOnly
    public synchronized void setInterceptor(@Nullable FailureHandler interceptor) {
        this.interceptor = interceptor;
    }

    /**
     * Returns failure handler.
     *
     * @return Failure handler.
     */
    FailureHandler handler() {
        return handler;
    }

    /**
     * Defines whether thread dump should be throttled for given failure type or not.
     * This method should be called under synchronization, see {@link #process(FailureContext, FailureHandler)},
     * because it can modify throttling timeout for the given failure type {@link #threadDumpPerFailureTypeTs}.
     *
     * @param failureContext Failure context.
     * @return {@code true} if thread dump generation should be throttled for given failure type.
     */
    private boolean throttleThreadDump(FailureContext failureContext) {
        FailureType type = failureContext.type();
        Map<FailureType, Long> dumpPerFailureTypeTs = threadDumpPerFailureTypeTs;
        long dumpThrottlingTimeout = dumpThreadsThrottlingTimeout;

        if (dumpThrottlingTimeout == 0 || dumpPerFailureTypeTs == null) {
            return false;
        }

        long curr = System.currentTimeMillis();

        Long last = dumpPerFailureTypeTs.get(type);

        assert last != null : "Unknown failure type " + type;

        boolean throttle = curr - last < dumpThrottlingTimeout;

        if (!throttle) {
            dumpPerFailureTypeTs.put(type, curr);
        } else {
            LOG.info("Thread dump is hidden due to throttling settings. "
                    + "Set 'dumpThreadsThrottlingTimeoutMillis' property to 0 to see all thread dumps "
                    + "[failureCtxId={}].",
                    failureContext.id()
            );
        }

        return throttle;
    }

    private class FailureHandlerConfigurationListener implements ConfigurationListener<FailureProcessorView> {
        @Override
        public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<FailureProcessorView> ctx) {
            reconfigure(ctx.newValue());

            return nullCompletedFuture();
        }
    }
}
