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

package org.apache.ignite.internal.table.distributed;

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.IntFunction;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesView;

/**
 * Caches IDs of the tables that are currently configured in a way consistent with creation/removal
 * of tables: that is, if the set of configured table IDs changes externally (in the configuration),
 * the cache will not return a stale result.
 */
// TODO: IGNITE-19226 - remove this
class ConfiguredTablesCache {
    private static final int NO_GENERATION = Integer.MIN_VALUE;

    private final TablesConfiguration tablesConfig;
    private final boolean getMetadataLocallyOnly;

    private int cachedGeneration = NO_GENERATION;
    private final IntSet configuredTableIds = new IntRBTreeSet();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final IntFunction<Boolean> isTableConfigured = configuredTableIds::contains;

    private final IntFunction<List<Integer>> getConfiguredTableIds = unused -> new ArrayList<>(configuredTableIds);

    ConfiguredTablesCache(TablesConfiguration tablesConfig, boolean getMetadataLocallyOnly) {
        this.tablesConfig = tablesConfig;
        this.getMetadataLocallyOnly = getMetadataLocallyOnly;
    }

    /**
     * Returns whether the table is present in the configuration.
     *
     * @param tableId ID of the table.
     * @return Whether the table is present in the configuration.
     */
    public boolean isTableConfigured(int tableId) {
        return getConsistently(tableId, isTableConfigured);
    }

    /**
     * Returns all configured table IDs.
     *
     * @return All configured table IDs.
     */
    public List<Integer> configuredTableIds() {
        return getConsistently(0, getConfiguredTableIds);
    }

    private <T> T getConsistently(int intArg, IntFunction<T> getter) {
        int currentGeneration = getCurrentGeneration();

        lock.readLock().lock();

        try {
            if (cachedGenerationMatches(currentGeneration)) {
                return getter.apply(intArg);
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();

        try {
            // Check again.
            currentGeneration = getCurrentGeneration();

            if (cachedGenerationMatches(currentGeneration)) {
                return getter.apply(intArg);
            }

            refillCache();

            return getter.apply(intArg);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Integer getCurrentGeneration() {
        return tablesConfigOrDirectProxy().tablesGeneration().value();
    }

    private TablesConfiguration tablesConfigOrDirectProxy() {
        return getMetadataLocallyOnly ? tablesConfig : tablesConfig.directProxy();
    }

    private boolean cachedGenerationMatches(int currentGeneration) {
        return cachedGeneration != NO_GENERATION && cachedGeneration == currentGeneration;
    }

    private void refillCache() {
        TablesView tablesView = tablesConfigOrDirectProxy().value();

        configuredTableIds.clear();

        for (TableView tableView : tablesView.tables()) {
            configuredTableIds.add(tableView.id());
        }

        cachedGeneration = tablesView.tablesGeneration();
    }
}
