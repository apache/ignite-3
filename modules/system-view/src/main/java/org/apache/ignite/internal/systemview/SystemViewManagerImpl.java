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

package org.apache.ignite.internal.systemview;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.cluster.management.NodeAttributesProvider;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.apache.ignite.internal.systemview.utils.SystemViewUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ErrorGroups.Common;

/**
 * SQL system views manager implementation.
 */
public class SystemViewManagerImpl implements SystemViewManager, NodeAttributesProvider, LogicalTopologyEventListener {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(SystemViewManagerImpl.class);

    public static final String NODE_ATTRIBUTES_KEY = "sql-system-views";

    public static final String NODE_ATTRIBUTES_LIST_SEPARATOR = ",";

    private final String localNodeName;

    private final CatalogManager catalogManager;

    private final Map<String, String> nodeAttributes = new HashMap<>();

    /** Collection of system views provided by components. */
    private final Map<String, SystemView<?>> views = new LinkedHashMap<>();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents attempts to register a new system view after the component has started. */
    private final AtomicBoolean startGuard = new AtomicBoolean();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Future which is completed when system views are registered in the catalog. */
    private final CompletableFuture<Void> viewsRegistrationFuture = new CompletableFuture<>();

    private volatile Map<String, List<String>> owningNodesByViewName = Map.of();

    /** Creates a system view manager. */
    public SystemViewManagerImpl(String localNodeName, CatalogManager catalogManager) {
        this.localNodeName = localNodeName;
        this.catalogManager = catalogManager;
    }

    @Override
    public void start() {
        inBusyLock(busyLock, () -> {
            if (!startGuard.compareAndSet(false, true)) {
                throw new IllegalStateException("System view manager cannot be started twice");
            }

            if (views.isEmpty()) {
                viewsRegistrationFuture.complete(null);

                return;
            }

            List<CatalogCommand> commands = views.values().stream()
                    .map(SystemViewUtils::toSystemViewCreateCommand)
                    .collect(Collectors.toList());

            catalogManager.execute(commands).whenComplete(
                    (r, t) -> {
                        viewsRegistrationFuture.complete(null);

                        if (t != null) {
                            LOG.warn("Failed to register system views.", t);
                        }
                    }
            );

            nodeAttributes.put(NODE_ATTRIBUTES_KEY, String.join(NODE_ATTRIBUTES_LIST_SEPARATOR, views.keySet()));
        });
    }

    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        viewsRegistrationFuture.completeExceptionally(new NodeStoppingException());

        busyLock.block();
    }

    @Override
    public List<String> owningNodes(String name) {
        return inBusyLock(busyLock, () -> owningNodesByViewName.getOrDefault(name, List.of()));
    }

    @Override
    public Publisher<InternalTuple> scanView(String name) {
        if (views.get(name) == null) {
            throw new IgniteInternalException(
                    Common.INTERNAL_ERR,
                    format("View with name '{}' not found on node '{}'", name, localNodeName)
            );
        }

        throw new UnsupportedOperationException("https://issues.apache.org/jira/browse/IGNITE-20578");
    }

    /** {@inheritDoc} */
    @Override
    public void register(SystemView<?> view) {
        if (views.containsKey(view.name())) {
            throw new IllegalArgumentException(format("The view with name '{}' already registered", view.name()));
        }

        inBusyLock(busyLock, () -> {
            if (startGuard.get()) {
                throw new IllegalStateException(format("Unable to register view '{}', manager already started", view.name()));
            }

            views.put(view.name(), view);
        });
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, String> nodeAttributes() {
        return nodeAttributes;
    }

    @Override
    public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
        processNewTopology(newTopology);
    }

    @Override
    public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
        processNewTopology(newTopology);
    }

    @Override
    public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
        processNewTopology(newTopology);
    }

    /**
     * Returns future which is completed when system views are registered in the catalog.
     */
    public CompletableFuture<Void> completeRegistration() {
        return viewsRegistrationFuture;
    }

    private void processNewTopology(LogicalTopologySnapshot topology) {
        Map<String, List<String>> owningNodesByViewName = new HashMap<>();

        for (LogicalNode logicalNode : topology.nodes()) {
            String systemViewsNames = logicalNode.systemAttributes().get(NODE_ATTRIBUTES_KEY);

            if (systemViewsNames == null) {
                continue;
            }

            Arrays.stream(systemViewsNames.split(NODE_ATTRIBUTES_LIST_SEPARATOR))
                    .map(String::trim)
                    .forEach(viewName ->
                            owningNodesByViewName.computeIfAbsent(viewName, key -> new ArrayList<>()).add(logicalNode.name())
                    );
        }

        for (String viewName : owningNodesByViewName.keySet()) {
            owningNodesByViewName.compute(viewName, (key, value) -> {
                assert value != null;

                return List.copyOf(value);
            });
        }

        this.owningNodesByViewName = Map.copyOf(owningNodesByViewName);
    }
}
