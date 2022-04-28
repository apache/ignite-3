/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.util.ReverseIterator;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;

/**
 * Class for managing the lifecycle of Ignite components.
 */
class LifecycleManager {
    private static final IgniteLogger LOG = IgniteLogger.forClass(LifecycleManager.class);

    /** Ignite node name. */
    private final String nodeName;

    /** Node status. Needed for handling stop a starting node. */
    private final AtomicReference<Status> status = new AtomicReference<>(Status.STARTING);

    /**
     * List of started Ignite components.
     *
     * <p>Multi-threaded access is guarded by {@code this}.
     */
    private final List<IgniteComponent> startedComponents = new ArrayList<>();

    /** Node state. */
    private enum Status {
        STARTING,
        STARTED,
        STOPPING
    }

    LifecycleManager(String nodeName) {
        this.nodeName = nodeName;
    }

    /**
     * Starts a given components unless the node is in {@link Status#STOPPING} state, in which case a {@link NodeStoppingException} will be
     * thrown.
     *
     * @param component Ignite component to start.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    void startComponent(IgniteComponent component) throws NodeStoppingException {
        if (status.get() == Status.STOPPING) {
            throw new NodeStoppingException("Node=[" + nodeName + "] was stopped");
        }

        synchronized (this) {
            startedComponents.add(component);

            component.start();
        }
    }

    /**
     * Similar to {@link #startComponent} but allows to start multiple components at once.
     *
     * @param components Ignite components to start.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    void startComponents(IgniteComponent... components) throws NodeStoppingException {
        for (IgniteComponent component : components) {
            startComponent(component);
        }
    }

    /**
     * Callback that should be called after the node has joined a cluster. After this method completes,
     * the node will be transferred to the {@link Status#STARTED} state.
     *
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    void onStartComplete() throws NodeStoppingException {
        Status currentStatus = status.compareAndExchange(Status.STARTING, Status.STARTED);

        if (currentStatus == Status.STOPPING) {
            throw new NodeStoppingException();
        } else if (currentStatus != Status.STARTING) {
            throw new IllegalStateException("Unexpected node status: " + currentStatus);
        }
    }

    /**
     * Stops all started components and transfers the node into the {@link Status#STOPPING} state.
     */
    void stopNode() {
        Status currentStatus = status.getAndSet(Status.STOPPING);

        if (currentStatus != Status.STOPPING) {
            stopAllComponents();
        }
    }

    /**
     * Calls {@link IgniteComponent#beforeNodeStop()} and then {@link IgniteComponent#stop()} for all components in start-reverse-order.
     */
    private synchronized void stopAllComponents() {
        new ReverseIterator<>(startedComponents).forEachRemaining(component -> {
            try {
                component.beforeNodeStop();
            } catch (Exception e) {
                LOG.error("Unable to execute before node stop on the component=[{}] within node=[{}]", e, component, nodeName);
            }
        });

        new ReverseIterator<>(startedComponents).forEachRemaining(component -> {
            try {
                component.stop();
            } catch (Exception e) {
                LOG.error("Unable to stop component=[{}] within node=[{}]", e, component, nodeName);
            }
        });
    }
}
