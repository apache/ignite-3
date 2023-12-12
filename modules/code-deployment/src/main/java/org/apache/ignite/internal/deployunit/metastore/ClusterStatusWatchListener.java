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

package org.apache.ignite.internal.deployunit.metastore;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;

/**
 * Cluster statuses store watch listener.
 */
public class ClusterStatusWatchListener implements WatchListener {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterStatusWatchListener.class);

    private final ClusterEventCallback clusterEventCallback;

    public ClusterStatusWatchListener(ClusterEventCallback clusterEventCallback) {
        this.clusterEventCallback = clusterEventCallback;
    }

    @Override
    public CompletableFuture<Void> onUpdate(WatchEvent event) {
        for (EntryEvent e : event.entryEvents()) {
            byte[] value = e.newEntry().value();
            if (value != null) {
                UnitClusterStatus unitStatus = UnitClusterStatus.deserialize(value);
                clusterEventCallback.onUpdate(unitStatus);
            }
        }
        return nullCompletedFuture();
    }

    @Override
    public void onError(Throwable e) {
        LOG.warn("Failed to process metastore deployment unit event. ", e);
    }
}
