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

package org.apache.ignite.internal.app;

import static java.util.Collections.reverse;
import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.rest.api.node.State;
import org.apache.ignite.internal.rest.node.StateProvider;

/**
 * Class for managing the lifecycle of Ignite components.
 */
class LifecycleManager implements StateProvider {
    private static final IgniteLogger LOG = Loggers.forClass(LifecycleManager.class);

    /** Ignite node name. */
    private final String nodeName;

    /** Node status. Needed for handling stop a starting node. */
    private final AtomicReference<State> status = new AtomicReference<>(State.STARTING);

    /**
     * List of started Ignite components.
     *
     * <p>Multi-threaded access is guarded by {@code this}.
     */
    private final List<IgniteComponent> startedComponents = new ArrayList<>();

    private final List<CompletableFuture<Void>> allComponentsStartFuture = new ArrayList<>();

    private final CompletableFuture<Void> stopFuture = new CompletableFuture<>();

    LifecycleManager(String nodeName) {
        this.nodeName = nodeName;
    }

    @Override
    public State getState() {
        return status.get();
    }

    /**
     * Starts a given components unless the node is in {@link State#STOPPING} state, in which case a {@link NodeStoppingException} will be
     * thrown.
     *
     * @param component Ignite component to start.
     * @param componentContext Component context.
     * @return Future that will be completed when the asynchronous part of the start is processed.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    CompletableFuture<Void> startComponentAsync(IgniteComponent component, ComponentContext componentContext)
            throws NodeStoppingException {
        if (status.get() == State.STOPPING) {
            throw new NodeStoppingException("Node=[" + nodeName + "] was stopped");
        }

        synchronized (this) {
            startedComponents.add(component);

            CompletableFuture<Void> future = component.startAsync(componentContext);
            allComponentsStartFuture.add(future);
            return future;
        }
    }

    /**
     * Similar to {@link #startComponentAsync} but allows to start multiple components at once.
     *
     * @param componentContext Component context.
     * @param components Ignite components to start.
     * @return Future that will be completed when all the components are started.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    CompletableFuture<Void> startComponentsAsync(ComponentContext componentContext, IgniteComponent... components)
            throws NodeStoppingException {
        CompletableFuture<?>[] futures = new CompletableFuture[components.length];
        for (int i = 0; i < components.length; i++) {
            futures[i] = startComponentAsync(components[i], componentContext);
        }
        return allOf(futures);
    }

    /**
     * Callback that should be called after the node has joined a cluster. After this method completes, the node will be transferred to the
     * {@link State#STARTED} state.
     *
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    void onStartComplete() throws NodeStoppingException {
        LOG.info("Start complete");

        State currentStatus = status.compareAndExchange(State.STARTING, State.STARTED);

        if (currentStatus == State.STOPPING) {
            throw new NodeStoppingException();
        } else if (currentStatus != State.STARTING) {
            throw new IllegalStateException("Unexpected node status: " + currentStatus);
        }
    }

    /**
     * Represents future that will be completed when all components start futures will be completed. Note that it is designed that this
     * method is called only once.
     *
     * @return Future that will be completed when all components start futures will be completed.
     */
    synchronized CompletableFuture<Void> allComponentsStartFuture() {
        return allOf(allComponentsStartFuture.toArray(CompletableFuture[]::new))
                .whenComplete((v, e) -> {
                    synchronized (this) {
                        allComponentsStartFuture.clear();
                    }
                });
    }

    /**
     * Stops all started components and transfers the node into the {@link State#STOPPING} state.
     *
     * @param componentContext Component context.
     */
    CompletableFuture<Void> stopNode(ComponentContext componentContext) {
        State currentStatus = status.getAndSet(State.STOPPING);

        if (currentStatus != State.STOPPING) {
            stopAllComponents(componentContext);
        }

        return stopFuture;
    }

    /**
     * Calls {@link IgniteComponent#beforeNodeStop()} and then {@link IgniteComponent#stopAsync(ComponentContext)} for all components in
     * start-reverse-order.
     *
     * @param componentContext Component context.
     */
    private synchronized void stopAllComponents(ComponentContext componentContext) {
        List<IgniteComponent> components = new ArrayList<>(startedComponents);
        reverse(components);

        for (IgniteComponent component : components) {
            try {
                component.beforeNodeStop();
            } catch (Exception e) {
                LOG.warn("Unable to execute before node stop [component={}, nodeName={}]", e, component, nodeName);
            }
        }

        stopAsync(componentContext, components)
                .whenComplete((v, e) -> stopFuture.complete(null));
    }
}
