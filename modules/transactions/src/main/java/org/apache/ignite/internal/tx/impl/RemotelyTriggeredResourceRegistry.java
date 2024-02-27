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

package org.apache.ignite.internal.tx.impl;

import static java.util.Collections.unmodifiableMap;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.network.ClusterNodeResolver;

/**
 * This registry keeps track of the resources that were created by remote nodes.
 */
public class RemotelyTriggeredResourceRegistry implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RemotelyTriggeredResourceRegistry.class);

    /** Resources map. */
    private final ConcurrentNavigableMap<FullyQualifiedResourceId, RemotelyTriggeredResource> resources = new ConcurrentSkipListMap<>();

    private final Map<String, Set<FullyQualifiedResourceId>> remoteHostsToResources = new ConcurrentHashMap<>();

    /** Cluster node resolver. */
    private final ClusterNodeResolver clusterNodeResolver;

    private final TxScheduledCleanupManager txScheduledCleanupManager;

    /**
     * Constructor.
     *
     * @param clusterNodeResolver Cluster node resolver.
     */
    public RemotelyTriggeredResourceRegistry(ClusterNodeResolver clusterNodeResolver, TxScheduledCleanupManager txScheduledCleanupManager) {
        this.clusterNodeResolver = clusterNodeResolver;
        this.txScheduledCleanupManager = txScheduledCleanupManager;
    }

    @Override
    public CompletableFuture<Void> start() {
        txScheduledCleanupManager.registerScheduledOperation(this::cleanupOrphanTxResources);

        return nullCompletedFuture();
    }

    @Override
    public void stop() throws Exception {
        // No-op.
    }

    /**
     * Register a resource.
     *
     * @param resourceId Resource id.
     * @param remoteHostId Node id of the creator of the resource.
     * @param resourceProvider Supplier to create a resource.
     * @return Registered resource.
     */
    public <T extends ManuallyCloseable> T register(
            FullyQualifiedResourceId resourceId,
            String remoteHostId,
            Supplier<ManuallyCloseable> resourceProvider
    ) {
        T r = (T) resources.computeIfAbsent(resourceId, k -> new RemotelyTriggeredResource(resourceProvider.get(), remoteHostId)).resource;

        addRemoteHostResource(remoteHostId, resourceId);

        return r;
    }

    /**
     * Close the given resource.
     *
     * @param resourceId Resource id.
     */
    public void close(FullyQualifiedResourceId resourceId) throws ResourceCloseException {
        RemotelyTriggeredResource remotelyTriggeredResource = resources.remove(resourceId);

        if (remotelyTriggeredResource != null) {
            try {
                remotelyTriggeredResource.resource.close();

                remoteRemoteHostResource(remotelyTriggeredResource.remoteHostId(), resourceId);
            } catch (Exception e) {
                throw new ResourceCloseException(resourceId, remotelyTriggeredResource.remoteHostId(), e);
            }
        }
    }

    /**
     * Close all resources sharing the same context id.
     *
     * @param contextId Context id of the resources.
     */
    public void close(UUID contextId) throws ResourceCloseException {
        Map<FullyQualifiedResourceId, RemotelyTriggeredResource> resourcesWithContext = resources(contextId);

        ResourceCloseException ex = null;

        Set<FullyQualifiedResourceId> closedResources = new HashSet<>();

        // We assume that the resources of the same context are triggered by the same remote host.
        String remoteHostId = null;

        for (Entry<FullyQualifiedResourceId, RemotelyTriggeredResource> entry : resourcesWithContext.entrySet()) {
            try {
                entry.getValue().resource.close();

                closedResources.add(entry.getKey());

                if (remoteHostId == null) {
                    remoteHostId = entry.getValue().remoteHostId();
                }

                assert remoteHostId == entry.getValue().remoteHostId() : "Resources of the same context triggered by different remote "
                        + "hosts [" + remoteHostId + ", " + entry.getValue().remoteHostId() + "].";
            } catch (Exception e) {
                if (ex == null) {
                    ex = new ResourceCloseException(entry.getKey(), entry.getValue().remoteHostId(), e);
                } else {
                    ex.addSuppressed(e);
                }
            }
        }

        if (!closedResources.isEmpty()) {
            assert remoteHostId != null : "Remote host is null, resources=" + resourcesWithContext;

            for (FullyQualifiedResourceId resourceId : closedResources) {
                resourcesWithContext.remove(resourceId);

                remoteRemoteHostResource(remoteHostId, resourceId);
            }
        }

        if (ex != null) {
            throw ex;
        }
    }

    private void addRemoteHostResource(String remoteHostId, FullyQualifiedResourceId resourceId) {
        remoteHostsToResources.compute(remoteHostId, (k, v) -> {
            if (v == null) {
                v = ConcurrentHashMap.newKeySet();
            }

            v.add(resourceId);

            return v;
        });
    }

    private void remoteRemoteHostResource(String remoteHostId, FullyQualifiedResourceId resourceId) {
        remoteHostsToResources.computeIfPresent(remoteHostId, (k, v) -> {
            v.remove(resourceId);

            return v.isEmpty() ? null : v;
        });
    }

    private Map<FullyQualifiedResourceId, RemotelyTriggeredResource> resources(UUID contextId) {
        var lowResourceId = FullyQualifiedResourceId.lower(contextId);
        var upperResourceId = FullyQualifiedResourceId.upper(contextId);

        return resources.subMap(lowResourceId, true, upperResourceId, true);
    }

    /**
     * Returns all resources.
     *
     * @return Remotely triggered resources.
     */
    public Map<FullyQualifiedResourceId, RemotelyTriggeredResource> resources() {
        return unmodifiableMap(resources);
    }

    private void cleanupOrphanTxResources() {
        try {
            for (Map.Entry<String, Set<FullyQualifiedResourceId>> remoteHostResourceEntry : remoteHostsToResources.entrySet()) {
                if (clusterNodeResolver.getById(remoteHostResourceEntry.getKey()) == null) {
                    for (FullyQualifiedResourceId resourceId : remoteHostResourceEntry.getValue()) {
                        try {
                            close(resourceId);
                        } catch (Exception e) {
                            LOG.warn("Error occurred during the orphan cursor closing.", e);
                        }
                    }
                }
            }
        } catch (Throwable err) {
            LOG.error("Error occurred during the orphan cursors closing.", err);

            if (err instanceof Error) {
                throw err;
            }
        }
    }

    /**
     * Resource information.
     */
    public static class RemotelyTriggeredResource {
        private final ManuallyCloseable resource;

        private final String remoteHostId;

        public RemotelyTriggeredResource(ManuallyCloseable resource, String remoteHostId) {
            this.resource = resource;
            this.remoteHostId = remoteHostId;
        }

        /**
         * Registered resource.
         *
         * @return Resource.
         */
        public ManuallyCloseable resource() {
            return resource;
        }

        /**
         * Node inconsistent id of the creator of the resource.
         *
         * @return Node id of the creator of the resource.
         */
        public String remoteHostId() {
            return remoteHostId;
        }
    }
}
