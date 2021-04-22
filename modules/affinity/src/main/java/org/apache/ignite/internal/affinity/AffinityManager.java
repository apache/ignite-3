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

package org.apache.ignite.internal.affinity;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.runner.LocalConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.util.SerializationUtils;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.WatchEvent;
import org.apache.ignite.metastorage.common.WatchListener;
import org.jetbrains.annotations.NotNull;

/**
 * Affinity manager is responsible for affinity function related logic including calculating affinity assignments.
 */
// TODO: IGNITE-14586 Remove @SuppressWarnings when implementation provided.
public class AffinityManager {
    /** Tables prefix for the metasorage. */
    public static final String INTERNAL_PREFIX = "internal.tables.";

    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(AffinityManager.class);

    /**
     * MetaStorage manager in order to watch private distributed affinity specific configuration,
     * cause ConfigurationManger handles only public configuration.
     */
    private final MetaStorageManager metaStorageMgr;

    /** Configuration manager in order to handle and listen affinity specific configuration.*/
    private final ConfigurationManager configurationMgr;

    /** Baseline manager. */
    private final BaselineManager baselineMgr;

    /** Affinity calculate subscription future. */
    private CompletableFuture<Long> affinityCalculateSubscriptionFut = null;

    /**
     * @param configurationMgr Configuration module.
     * @param metaStorageMgr Meta storage service.
     */
    public AffinityManager(
        ConfigurationManager configurationMgr,
        MetaStorageManager metaStorageMgr,
        BaselineManager baselineMgr
    ) {
        this.configurationMgr = configurationMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.baselineMgr = baselineMgr;

        String localMemberName = configurationMgr.configurationRegistry().getConfiguration(NetworkConfiguration.KEY)
            .name().value();

        configurationMgr.configurationRegistry().getConfiguration(LocalConfiguration.KEY)
            .metastorageMembers().listen(ctx -> {
                if (ctx.newValue() != null) {
                    if (hasMetastorageLocally(localMemberName, ctx.newValue()))
                        subscribeToCalculateAssignment();
                    else
                        unsubscribeToCalculateAssignment();
                }
            return CompletableFuture.completedFuture(null);
        });

        String[] metastorageMembers = configurationMgr.configurationRegistry().getConfiguration(LocalConfiguration.KEY)
            .metastorageMembers().value();

        if (hasMetastorageLocally(localMemberName, metastorageMembers))
            subscribeToCalculateAssignment();
    }

    /**
     * Tests a member has a distributed Metastorage peer.
     *
     * @param localMemberName Local member uniq name.
     * @param metastorageMembers Metastorage members names.
     * @return True if the member has Metastorage, false otherwise.
     */
    private boolean hasMetastorageLocally(String localMemberName, String[] metastorageMembers) {
        boolean isLocalNodeHasMetasorage = false;

        for (String name : metastorageMembers) {
            if (name.equals(localMemberName)) {
                isLocalNodeHasMetasorage = true;

                break;
            }
        }
        return isLocalNodeHasMetasorage;
    }

    /**
     * Subscribes to metastorage members update.
     */
    private void subscribeToCalculateAssignment() {
        assert affinityCalculateSubscriptionFut == null : "Affinity calculation already subscribed";

        String tableInternalPrefix = INTERNAL_PREFIX + "assignment.#";

        affinityCalculateSubscriptionFut = metaStorageMgr.registerWatch(new Key(tableInternalPrefix), new WatchListener() {
            @Override public boolean onUpdate(@NotNull Iterable<WatchEvent> events) {
                for (WatchEvent evt : events) {
                    if (ArrayUtils.empty(evt.newEntry().value())) {
                        String keyTail = evt.newEntry().key().toString().substring(INTERNAL_PREFIX.length());

                        String placeholderValue = keyTail.substring(0, keyTail.indexOf('.'));

                        UUID tblId = UUID.fromString(placeholderValue);

                        try {
                            String name = new String(metaStorageMgr.get(
                                new Key(INTERNAL_PREFIX + tblId.toString())).get()
                                .value(), StandardCharsets.UTF_8);

                            int partitions = configurationMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY)
                                .tables().get(name).partitions().value();
                            int replicas = configurationMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY)
                                .tables().get(name).replicas().value();

                            metaStorageMgr.put(evt.newEntry().key(), SerializationUtils.toBytes(
                                RendezvousAffinityFunction.assignPartitions(
                                    baselineMgr.nodes(),
                                    partitions,
                                    replicas,
                                    false,
                                    null
                                ))
                            );

                            LOG.info("Affinity manager calculated assignment for the table [name={}, tblId={}]",
                                name, tblId);
                        }
                        catch (InterruptedException | ExecutionException e) {
                            LOG.error("Failed to initialize affinity [key={}]",
                                evt.newEntry().key().toString(), e);
                        }
                    }
                }

                return false;
            }

            @Override public void onError(@NotNull Throwable e) {
                LOG.error("Metastorage listener issue", e);
            }
        });
    }

    /**
     * Unsubscribes a listener form the affinity calculation.
     */
    private void unsubscribeToCalculateAssignment() {
        if (affinityCalculateSubscriptionFut == null)
            return;

        try {
            Long subscriptionId = affinityCalculateSubscriptionFut.get();

            metaStorageMgr.unregisterWatch(subscriptionId);

            affinityCalculateSubscriptionFut = null;
        }
        catch (InterruptedException | ExecutionException e) {
            LOG.error("Couldn't unsubscribe for Metastorage updates", e);
        }
    }
}
