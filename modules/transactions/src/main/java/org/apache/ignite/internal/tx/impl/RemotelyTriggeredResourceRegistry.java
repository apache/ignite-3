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

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;
import org.apache.ignite.internal.close.ManuallyCloseable;

/**
 * This registry keeps track of the resources that were created by remote nodes.
 */
public class RemotelyTriggeredResourceRegistry {
    /**
     * Resources map.
     */
    private final ConcurrentNavigableMap<FullyQualifiedResourceId, RemotelyTriggeredResource> resources =
            new ConcurrentSkipListMap<>(FullyQualifiedResourceId.globalOrderComparator());

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
        return (T) resources.computeIfAbsent(resourceId, k -> new RemotelyTriggeredResource(resourceProvider.get(), remoteHostId)).resource;
    }

    /**
     * Close the given resource.
     *
     * @param resourceId Resource id.
     */
    public void close(FullyQualifiedResourceId resourceId) throws ResourceCloseException {
        RemotelyTriggeredResource remotelyTriggeredResource = resources.get(resourceId);

        if (remotelyTriggeredResource != null) {
            try {
                remotelyTriggeredResource.resource.close();

                resources.remove(resourceId);
            } catch (Exception e) {
                throw new ResourceCloseException("Close resource exception.", resourceId, e);
            }
        }
    }

    /**
     * Close all cursors created sharing the same prefix.
     *
     * @param contextId Context id of the resources.
     */
    public void close(UUID contextId) throws ResourceCloseException {
        Map<FullyQualifiedResourceId, RemotelyTriggeredResource> resourcesWithContext = resources(contextId);

        ResourceCloseException ex = null;

        Set<FullyQualifiedResourceId> closedResources = new HashSet<>();

        for (Entry<FullyQualifiedResourceId, RemotelyTriggeredResource> entry : resourcesWithContext.entrySet()) {
            try {
                entry.getValue().resource.close();

                closedResources.add(entry.getKey());
            } catch (Exception e) {
                if (ex == null) {
                    ex = new ResourceCloseException("Close resource exception.", entry.getKey(), e);
                } else {
                    ex.addSuppressed(e);
                }
            }
        }

        resourcesWithContext.keySet().removeAll(closedResources);

        if (ex != null) {
            throw ex;
        }
    }

    private Map<FullyQualifiedResourceId, RemotelyTriggeredResource> resources(UUID cursorIdPrefix) {
        var lowCursorId = new FullyQualifiedResourceId(cursorIdPrefix, Long.MIN_VALUE);
        var upperCursorId = new FullyQualifiedResourceId(cursorIdPrefix, Long.MAX_VALUE);

        return resources.subMap(lowCursorId, true, upperCursorId, true);
    }

    /**
     * Returns all cursors.
     *
     * @return Cursors.
     */
    public Map<FullyQualifiedResourceId, RemotelyTriggeredResource> resources() {
        return unmodifiableMap(resources);
    }

    /**
     * Cursor information.
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
