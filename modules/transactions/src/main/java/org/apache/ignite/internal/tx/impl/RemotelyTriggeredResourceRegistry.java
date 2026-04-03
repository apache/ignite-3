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
import static java.util.Collections.unmodifiableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.TestOnly;

/**
 * This registry keeps track of the resources that were created by remote nodes.
 */
public class RemotelyTriggeredResourceRegistry {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RemotelyTriggeredResourceRegistry.class);

    /** Resources map: contextId -> (resourceId -> resource). */
    private final ConcurrentHashMap<UUID, ConcurrentHashMap<UUID, RemotelyTriggeredResource>> resources =
            new ConcurrentHashMap<>();

    /** Reverse index: remoteHostId -> set of contextIds registered by that host. */
    private final ConcurrentHashMap<UUID, Set<UUID>> remoteHostToContextIds = new ConcurrentHashMap<>();

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
            UUID remoteHostId,
            Supplier<ManuallyCloseable> resourceProvider
    ) {
        UUID contextId = Objects.requireNonNull(resourceId.contextId(), "contextId must not be null");

        ConcurrentHashMap<UUID, RemotelyTriggeredResource> innerMap =
                resources.computeIfAbsent(contextId, k -> new ConcurrentHashMap<>());

        @SuppressWarnings("unchecked")
        T r = (T) innerMap.computeIfAbsent(
                resourceId.resourceId(),
                k -> new RemotelyTriggeredResource(resourceProvider.get(), remoteHostId)
        ).resource;

        remoteHostToContextIds.computeIfAbsent(remoteHostId, k -> ConcurrentHashMap.newKeySet()).add(contextId);

        return r;
    }

    /**
     * Close the given resource.
     *
     * @param resourceId Resource id.
     */
    public void close(FullyQualifiedResourceId resourceId) throws ResourceCloseException {
        UUID contextId = resourceId.contextId();

        ConcurrentHashMap<UUID, RemotelyTriggeredResource> innerMap = resources.get(contextId);

        if (innerMap == null) {
            return;
        }

        RemotelyTriggeredResource resource = innerMap.get(resourceId.resourceId());

        if (resource == null) {
            return;
        }

        try {
            resource.resource.close();

            innerMap.remove(resourceId.resourceId());

            if (innerMap.isEmpty()) {
                resources.remove(contextId, innerMap);

                removeRemoteHostContext(resource.remoteHostId(), contextId);
            }
        } catch (Exception e) {
            throw new ResourceCloseException(resourceId, resource.remoteHostId(), e);
        }
    }

    /**
     * Close all resources sharing the same context id.
     *
     * @param contextId Context id of the resources.
     */
    public void close(UUID contextId) throws ResourceCloseException {
        ConcurrentHashMap<UUID, RemotelyTriggeredResource> innerMap = resources.get(contextId);

        if (innerMap == null) {
            return;
        }

        ResourceCloseException ex = null;
        Set<UUID> closedResourceIds = new HashSet<>();

        // We assume that the resources of the same context are triggered by the same remote host.
        UUID remoteHostId = null;

        for (Map.Entry<UUID, RemotelyTriggeredResource> entry : innerMap.entrySet()) {
            try {
                entry.getValue().resource.close();

                closedResourceIds.add(entry.getKey());

                if (remoteHostId == null) {
                    remoteHostId = entry.getValue().remoteHostId();
                }

                if (!remoteHostId.equals(entry.getValue().remoteHostId())) {
                    throw new IllegalStateException("Resources of the same context triggered by different remote "
                            + "hosts [" + remoteHostId + ", " + entry.getValue().remoteHostId() + "].");
                }
            } catch (Exception e) {
                FullyQualifiedResourceId fqId = new FullyQualifiedResourceId(contextId, entry.getKey());

                if (ex == null) {
                    ex = new ResourceCloseException(fqId, entry.getValue().remoteHostId(), e);
                } else {
                    ex.addSuppressed(e);
                }
            }
        }

        if (!closedResourceIds.isEmpty()) {
            assert remoteHostId != null : "Remote host is null, contextId=" + contextId;

            for (UUID resourceId : closedResourceIds) {
                innerMap.remove(resourceId);
            }

            if (innerMap.isEmpty()) {
                resources.remove(contextId, innerMap);

                removeRemoteHostContext(remoteHostId, contextId);
            }
        }

        if (ex != null) {
            throw ex;
        }
    }

    /**
     * Close all resources created by the given remote host.
     *
     * @param remoteHostId Remote host inconsistent id.
     */
    public void closeByRemoteHostId(UUID remoteHostId) {
        Set<UUID> contextIds = remoteHostToContextIds.remove(remoteHostId);

        if (contextIds == null) {
            // Remote host resources were already closed or no resources were registered for this host.
            return;
        }

        for (UUID contextId : contextIds) {
            try {
                close(contextId);
            } catch (Exception e) {
                LOG.warn("Exception occurred during the orphan resource closing [contextId={}].", e, contextId);
            }
        }
    }

    private void removeRemoteHostContext(UUID remoteHostId, UUID contextId) {
        remoteHostToContextIds.computeIfPresent(remoteHostId, (k, v) -> {
            v.remove(contextId);

            return v.isEmpty() ? null : v;
        });
    }

    /**
     * Returns all resources as a flat map.
     *
     * @return Remotely triggered resources.
     */
    @TestOnly
    public Map<FullyQualifiedResourceId, RemotelyTriggeredResource> resources() {
        Map<FullyQualifiedResourceId, RemotelyTriggeredResource> flat = new HashMap<>();

        resources.forEach((contextId, innerMap) ->
                innerMap.forEach((resourceId, resource) ->
                        flat.put(new FullyQualifiedResourceId(contextId, resourceId), resource)));

        return unmodifiableMap(flat);
    }

    /**
     * Inconsistent ids of remote hosts that created the resources.
     *
     * @return Remote host inconsistent ids.
     */
    Set<UUID> registeredRemoteHosts() {
        return unmodifiableSet(remoteHostToContextIds.keySet());
    }

    /**
     * Resource information.
     */
    public static class RemotelyTriggeredResource {
        private final ManuallyCloseable resource;

        private final UUID remoteHostId;

        public RemotelyTriggeredResource(ManuallyCloseable resource, UUID remoteHostId) {
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
        public UUID remoteHostId() {
            return remoteHostId;
        }
    }
}
