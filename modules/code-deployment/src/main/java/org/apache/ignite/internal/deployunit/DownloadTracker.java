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

package org.apache.ignite.internal.deployunit;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.metastore.status.ClusterStatusKey;

/**
 * Deploy actions tracker.
 */
public class DownloadTracker {
    /**
     * In flight futures tracker.
     */
    private final Map<ClusterStatusKey, CompletableFuture<?>> inFlightFutures = new ConcurrentHashMap<>();

    /**
     * Track deploy action.
     *
     * @param <T> Future result type.
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @param trackableAction Deploy action.
     * @return {@param trackableAction}.
     */
    public <T> CompletableFuture<T> track(String id, Version version, Supplier<CompletableFuture<T>> trackableAction) {
        ClusterStatusKey key = ClusterStatusKey.builder().id(id).version(version).build();
        return (CompletableFuture<T>) inFlightFutures.computeIfAbsent(key, k -> trackableAction.get());
    }

    /**
     * Cancel deploy action for deployment unit with provided id and version if exists.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment version identifier.
     */
    public void cancelIfDownloading(String id, Version version) {
        ClusterStatusKey key = ClusterStatusKey.builder().id(id).version(version).build();
        CompletableFuture<?> future = inFlightFutures.remove(key);
        if (future != null) {
            future.cancel(true);
        }
    }

    /**
     * Cancel all deploy actions.
     */
    public void cancelAll() {
        inFlightFutures.values().forEach(future -> future.cancel(true));
    }
}
