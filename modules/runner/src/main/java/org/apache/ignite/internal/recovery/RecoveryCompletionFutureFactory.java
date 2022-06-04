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

package org.apache.ignite.internal.recovery;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.configuration.notifications.ConfigurationStorageRevisionListener;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.event.MetaStorageEvent;
import org.apache.ignite.internal.metastorage.event.MetaStorageEventParameters;
import org.apache.ignite.lang.IgniteLogger;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Creates a future that completes when local recovery is finished.
 */
public class RecoveryCompletionFutureFactory {
    /**
     * Create recovery completion future.
     *
     * @param clusterCfgMgr Cluster configuration manager.
     * @param listenerProvider Provider of configuration listener.
     * @return Recovery completion future.
     */
    public static CompletableFuture<?> create(
            MetaStorageManager metaStorageManager,
            ConfigurationStorage cfgStorage,
            Function<CompletableFuture<?>, EventListener<MetaStorageEventParameters>> listenerProvider
    ) {
        return metaStorageManager.appliedRevision()
            .thenCombine(cfgStorage.lastRevision(), (appliedRevision, lastRevision) -> {
                if (appliedRevision >= lastRevision) {
                    System.out.println("qqq Applied revision is not behind the last revision, completing recovery.");

                    return completedFuture(null);
                }

                CompletableFuture<?> configCatchUpFuture = new CompletableFuture<>();

                EventListener<MetaStorageEventParameters> listener = listenerProvider.apply(configCatchUpFuture);

                metaStorageManager.listen(MetaStorageEvent.REVISION_APPLIED, listener);

                return configCatchUpFuture;
            })
            .thenCompose(f -> f);
    }
}
