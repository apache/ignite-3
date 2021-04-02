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
import java.util.concurrent.ExecutionException;
import org.apache.ignite.baseline.internal.BaselineManager;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.internal.affinity.RendezvousAffinityFunction;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.lang.util.SerializationUtils;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.WatchEvent;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.metastorage.configuration.MetastoreManagerConfiguration;
import org.apache.ignite.metastorage.internal.MetaStorageManager;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.table.distributed.configuration.DistributedTableConfiguration;
import org.jetbrains.annotations.NotNull;

public class AffinityManager {
    /** Tables prefix for the metasorage. */
    public static final String INTERNAL_PREFIX = "internal.tables.";

    /** Meta storage service. */
    private MetaStorageManager metaStorageMgr;

    /** Network cluster. */
    private NetworkCluster networkCluster;

    /** Configuration module. */
    private ConfigurationManager configurationMgr;

    /** Baseline manager. */
    private BaselineManager baselineMgr;

    /** Logger. */
    private LogWrapper log = new LogWrapper(AffinityManager.class);

    /**
     * @param configurationMgr Configuration module.
     * @param networkCluster Network cluster.
     * @param metaStorageMgr Meta storage service.
     */
    public AffinityManager(
        ConfigurationManager configurationMgr,
        NetworkCluster networkCluster,
        MetaStorageManager metaStorageMgr,
        BaselineManager baselineMgr
    ) {
        int startRevision =0;

        this.configurationMgr = configurationMgr;
        this.networkCluster = networkCluster;
        this.metaStorageMgr = metaStorageMgr;
        this.baselineMgr = baselineMgr;

        String[] metastoragePeerNames = configurationMgr.configurationRegistry()
            .getConfiguration(MetastoreManagerConfiguration.KEY).names().value();

        NetworkMember localMember = networkCluster.localMember();

        boolean isLocalNodeHasMetasorage = false;

        for (String name : metastoragePeerNames) {
            if (name.equals(localMember.name())) {
                isLocalNodeHasMetasorage = true;

                break;
            }
        }

        if (isLocalNodeHasMetasorage) {
            String tableInternalPrefix = INTERNAL_PREFIX + "#.assignment";

            metaStorageMgr.service().watch(new Key(tableInternalPrefix), startRevision, new WatchListener() {
                @Override public boolean onUpdate(@NotNull Iterable<WatchEvent> events) {
                    for (WatchEvent evt : events) {
                        if (evt.newEntry().value() == null) {
                            String keyTail = evt.newEntry().key().toString().substring(INTERNAL_PREFIX.length());

                            String placeholderValue = keyTail.substring(0, keyTail.indexOf('.'));

                            UUID tblId = UUID.fromString(placeholderValue);

                            try {
                                String name = new String(metaStorageMgr.service().get(
                                    new Key(INTERNAL_PREFIX + tblId.toString())).get()
                                    .value(), StandardCharsets.UTF_8);

                                int partitions = configurationMgr.configurationRegistry().getConfiguration(DistributedTableConfiguration.KEY)
                                    .tables().get(name).partitions().value();
                                int backups = configurationMgr.configurationRegistry().getConfiguration(DistributedTableConfiguration.KEY)
                                    .tables().get(name).backups().value();

                                metaStorageMgr.service().put(evt.newEntry().key(), SerializationUtils.toBytes(
                                    new RendezvousAffinityFunction(
                                        false,
                                        partitions
                                    ).assignPartitions(
                                        networkCluster.allMembers(),
                                        backups
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
    }
}
