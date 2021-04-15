/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.ignite.metastorage.internal;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.LocalConfiguration;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.metastorage.client.MetaStorageService;
import org.apache.ignite.metastorage.common.Condition;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.Operation;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkHandlersProvider;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.raft.internal.Loza;
import org.jetbrains.annotations.NotNull;

public class MetaStorageManager {
    private final NetworkCluster network;

    private final Loza raftMgr;

    private final ConfigurationManager locConfigurationMgr;

    private MetaStorageService service;

    public MetaStorageManager(
        NetworkCluster network,
        Loza raftMgr,
        ConfigurationManager locConfigurationMgr
    ) {
        this.network = network;
        this.raftMgr = raftMgr;
        this.locConfigurationMgr = locConfigurationMgr;

        locConfigurationMgr.configurationRegistry().getConfiguration(ClusterConfiguration.KEY)
            .metastorageMembers().listen(ctx -> {
            if (ctx.newValue() != null) {
                String[] metastorageMembers = ctx.newValue();

                locConfigurationMgr.configurationRegistry().getConfiguration(LocalConfiguration.KEY).change(change -> {
                    change.changeMetastorageMembers(metastorageMembers);
                });
            }

            return CompletableFuture.completedFuture(null);
        });

        network.addHandlersProvider(new NetworkHandlersProvider() {
            @Override public NetworkMessageHandler messageHandler() {
                return (message, sender, corellationId) -> {
                    // TODO sanpwc: Add MetaStorageMessageTypes.CLUSTER_INIT_REQUEST message handler.
                };
            }
        });
    }

    /**
     * This is a lazy subscribing to metastorage update.
     *
     * @param key Key in metastorage.
     * @param revision Revision.
     * @param lsnr Listener.
     * @return A future which will complete when the listener would be subscribed.
     */
    public CompletableFuture<IgniteUuid> watch(
        @NotNull Key key,
        long revision,
        @NotNull WatchListener lsnr
    ) {
        //TODO: Subscribe it later.
        return CompletableFuture.completedFuture(new IgniteUuid(UUID.randomUUID(), 0));
    }

    /**
     * Stops watching metastorage update.
     *
     * @param id Subscription id.
     * @return Future.
     */
    @NotNull
    public CompletableFuture<Void> stopWatch(@NotNull IgniteUuid id) {
        return service.stopWatch(id);
    }

    /**
     * Invokes a service operation for metastorage.
     *
     * @param key Key in metastorage.
     * @param condition Condition to process.
     * @param success Success operation.
     * @param failure Failure operation.
     * @return Future which will complete when appropriate final operation would be invoked.
     */
    public CompletableFuture<Boolean> invoke(@NotNull Key key, @NotNull Condition condition,
        @NotNull Operation success, @NotNull Operation failure) {
        return service.invoke(key, condition, success, failure);
    }

    /**
     * Gets a metastorage entry by key.
     *
     * @param key Key in metastorage.
     * @param revision Metastorage update revision.
     * @return Future.
     */
    public CompletableFuture<Entry> get(@NotNull Key key, long revision) {
        return service.get(key, revision);
    }

    /**
     * Puts an entry to metastorage.
     *
     * @param key Key in metastorage.
     * @param value Value bytes.
     * @return Future.
     */
    public CompletableFuture<Void> put(@NotNull Key key, @NotNull byte[] value) {
        return service.put(key, value);
    }


    public void registerWatch() {

    }

    public void deployWatches() {

    }
}

