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

package org.apache.ignite.internal.affinity.ditributed;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.baseline.internal.BaselineManager;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.configuration.schemas.runner.LocalConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.affinity.RendezvousAffinityFunction;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.lang.util.SerializationUtils;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.WatchEvent;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.metastorage.internal.MetaStorageManager;
import org.jetbrains.annotations.NotNull;

public class AffinityManager {
    /** Tables prefix for the metasorage. */
    public static final String INTERNAL_PREFIX = "internal.tables.";

    /** Meta storage service. */
    private final MetaStorageManager metaStorageMgr;

    /** Configuration manager. */
    private final ConfigurationManager configurationMgr;

    /** Baseline manager. */
    private final BaselineManager baselineMgr;

    /** Logger. */
    private final LogWrapper log = new LogWrapper(AffinityManager.class);

    /** Affinity calculate subscription future. */
    private CompletableFuture<IgniteUuid> affinityCalculateSubscriptionFut = null;

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

        long startRevision = 0;

        String localMemberName = configurationMgr.configurationRegistry().getConfiguration(LocalConfiguration.KEY)
            .name().value();

        configurationMgr.configurationRegistry().getConfiguration(LocalConfiguration.KEY)
            .metastorageMembers().listen(ctx -> {
                if (ctx.newValue() != null) {
                    if (hasMetastorageLocally(localMemberName, ctx.newValue()))
                        subscribeToCalculateAssignment(ctx.storageRevision());
                    else
                        unsubscribeToCalculateAssignment();
                }
            return CompletableFuture.completedFuture(null);
        });

        String[] metastorageMembers = configurationMgr.configurationRegistry().getConfiguration(LocalConfiguration.KEY)
            .metastorageMembers().value();

        if (hasMetastorageLocally(localMemberName, metastorageMembers))
            subscribeToCalculateAssignment(startRevision);
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
     *
     * @param startRevision Metastorage revision to start subscription.
     */
    private void subscribeToCalculateAssignment(long startRevision) {
        assert affinityCalculateSubscriptionFut == null : "Affinity calculation already subscribed";

        String tableInternalPrefix = INTERNAL_PREFIX + "#.assignment";

        affinityCalculateSubscriptionFut = metaStorageMgr.watch(new Key(tableInternalPrefix), startRevision, new WatchListener() {
            @Override public boolean onUpdate(@NotNull Iterable<WatchEvent> events) {
                for (WatchEvent evt : events) {
                    if (evt.newEntry().value() == null) {
                        long evtRevision = evt.newEntry().revision();

                        String keyTail = evt.newEntry().key().toString().substring(INTERNAL_PREFIX.length());

                        String placeholderValue = keyTail.substring(0, keyTail.indexOf('.'));

                        UUID tblId = UUID.fromString(placeholderValue);

                        try {
                            String name = new String(metaStorageMgr.get(
                                new Key(INTERNAL_PREFIX + tblId.toString()), evtRevision).get()
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

                            log.info("Affinity manager calculated assignment for the table [name={}, tblId={}]",
                                name, tblId);
                        }
                        catch (InterruptedException | ExecutionException e) {
                            log.error("Failed to initialize affinity [key={}]",
                                evt.newEntry().key().toString(), e);
                        }
                    }
                }

                return false;
            }

            @Override public void onError(@NotNull Throwable e) {
                log.error("Metastorage listener issue", e);
            }
        });
    }

    private void unsubscribeToCalculateAssignment() {
        if (affinityCalculateSubscriptionFut == null)
            return;

        try {
            IgniteUuid subscriptionId = affinityCalculateSubscriptionFut.get();

            metaStorageMgr.stopWatch(subscriptionId);

            affinityCalculateSubscriptionFut = null;
        }
        catch (InterruptedException |ExecutionException e) {
            log.error("Couldn't unsubscribe for Metastorage updates", e);
        }
    }
}
