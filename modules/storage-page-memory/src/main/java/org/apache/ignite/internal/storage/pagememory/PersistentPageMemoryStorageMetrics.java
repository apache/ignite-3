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

package org.apache.ignite.internal.storage.pagememory;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.pagememory.persistence.store.GroupPageStoresMap.GroupPartitionPageStore;

/** Persistent page memory storage metrics. */
class PersistentPageMemoryStorageMetrics {
    private static final IgniteLogger LOG = Loggers.forClass(PersistentPageMemoryStorageMetrics.class);

    /** Initializes metrics in the given metric source. */
    static void initMetrics(
            PersistentPageMemoryStorageMetricSource source,
            FilePageStoreManager filePageStoreManager
    ) {
        source.addMetric(new LongGauge(
                "StorageSize",
                "Size of the entire storage in bytes.",
                () -> storageSize(filePageStoreManager)
        ));
    }

    private static long storageSize(FilePageStoreManager filePageStoreManager) {
        return filePageStoreManager.allPageStores().mapToLong(PersistentPageMemoryStorageMetrics::fullSize).sum();
    }

    private static long fullSize(GroupPartitionPageStore<FilePageStore> pageStore) {
        try {
            return pageStore.pageStore().fullSize();
        } catch (IgniteInternalCheckedException e) {
            LOG.warn("Error getting storage size: [groupPartitionId={}]", e, pageStore.groupPartitionId());

            return 0;
        }
    }
}
