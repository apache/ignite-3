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
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.deployunit.metastore.status.ClusterStatusKey;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.lang.ByteArray;

/**
 * Deploy actions tracker.
 */
public class DeployTracker {
    /**
     * In flight futures tracker.
     */
    private final Map<ByteArray, InFlightFutures> inFlightFutures = new ConcurrentHashMap<>();

    /**
     * Track deploy action.
     *
     * @param <T> Future result type.
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @param trackableAction Deploy action.
     * @return {@param trackableAction}.
     */
    public <T> CompletableFuture<T> track(String id, Version version, CompletableFuture<T> trackableAction) {
        ByteArray key = ClusterStatusKey.builder().id(id).version(version).build().toByteArray();
        return inFlightFutures.computeIfAbsent(key, k -> new InFlightFutures()).registerFuture(trackableAction);
    }

    /**
     * Cancel deploy action for deployment unit with provided id and version if exists.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment version identifier.
     */
    public void cancelIfDeploy(String id, Version version) {
        InFlightFutures futureTracker = inFlightFutures.get(ClusterStatusKey.builder().id(id).version(version).build().toByteArray());
        if (futureTracker != null) {
            futureTracker.cancelInFlightFutures();
        }
    }

    /**
     * Cancel all deploy actions.
     */
    public void cancelAll() {
        inFlightFutures.values().forEach(InFlightFutures::cancelInFlightFutures);
    }
}
