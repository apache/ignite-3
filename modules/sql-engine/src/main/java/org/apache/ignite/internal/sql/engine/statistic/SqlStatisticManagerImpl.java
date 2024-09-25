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

package org.apache.ignite.internal.sql.engine.statistic;

import static org.apache.ignite.internal.event.EventListener.fromConsumer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.FastTimestamps;
import org.apache.ignite.internal.util.Pair;
import org.jetbrains.annotations.TestOnly;

/**
 * Statistic manager. Provide and manage update of statistics for SQL.
 */
public class SqlStatisticManagerImpl implements SqlStatisticManager {
    private static final IgniteLogger LOG = Loggers.forClass(SqlStatisticManagerImpl.class);
    private static final long DEFAULT_TABLE_SIZE = 1_000_000;
    private static final long MINIMUM_TABLE_SIZE = 1_000L;
    private static final Pair<Long, Long> DEFAULT_VALUE = new Pair<>(DEFAULT_TABLE_SIZE, 0L);

    private final TableManager tableManager;

    private final ConcurrentMap<Integer, Pair<Long, Long>> tableSizeMap = new ConcurrentHashMap<>();

    private volatile long thresholdTimeToPostponeUpdateMs = TimeUnit.MINUTES.toMillis(1);

    /** Constructor. */
    public SqlStatisticManagerImpl(TableManager tableManager, CatalogManager catalogManager) {
        this.tableManager = tableManager;

        catalogManager.listen(CatalogEvent.TABLE_DROP, fromConsumer(event -> {
            int tableId = ((DropTableEventParameters) event).tableId();
            onTableDrop(tableId);
        }));
    }

    /**
     * Returns approximate number of rows in table by their id. Returns the previous known value or
     * {@value SqlStatisticManagerImpl#DEFAULT_TABLE_SIZE} as default value. Can start process to update asked statistics in background to
     * have updated values for future requests.
     */
    @Override
    public long tableSize(int tableId) {
        updateTableSizeStatistics(tableId);
        long ts = tableSizeMap.getOrDefault(tableId, DEFAULT_VALUE).getFirst();

        return Math.max(ts, MINIMUM_TABLE_SIZE);
    }

    /** Update table size statistic in the background if it required. */
    private void updateTableSizeStatistics(int tableId) {
        tableSizeMap.putIfAbsent(tableId, DEFAULT_VALUE);
        Pair<Long, Long> pair = tableSizeMap.get(tableId);
        long currTs = FastTimestamps.coarseCurrentTimeMillis();
        long lastUpdateTime = pair.getSecond();

        if (lastUpdateTime <= currTs - thresholdTimeToPostponeUpdateMs) {
            // Prevent to run update for the same table twice concurrently.
            if (!tableSizeMap.replace(tableId, pair, new Pair<>(pair.getFirst(), currTs))) {
                return;
            }

            TableViewInternal tableView = tableManager.cachedTable(tableId);
            if (tableView == null) {
                LOG.debug("There is no table to update statistics [id={}]", tableId);
                return;
            }

            // just request new table size in background.
            tableView.internalTable().estimatedSize()
                    .thenApply(size -> {
                        // the table can be concurrently dropped and we shouldn't put new value in this case.
                        tableSizeMap.computeIfPresent(tableId, (k, v) -> new Pair<>(size, currTs));
                        return size;
                    }).exceptionally(e -> {
                        LOG.info("Can't calculate size for table [id={}]", e, tableId);
                        return DEFAULT_TABLE_SIZE;
                    });
        }
    }

    /** Cleans up resources after a table is dropped. */
    private void onTableDrop(int tableId) {
        // There is no any guarantee that after delete statistics will be not asked again and cache will not contains obsolete data.
        tableSizeMap.remove(tableId);
    }

    /**
     * Set threshold time to postpone update statistics.
     */
    @TestOnly
    public long setThresholdTimeToPostponeUpdateMs(long milliseconds) {
        assert milliseconds >= 0;
        long prevValue = thresholdTimeToPostponeUpdateMs;
        thresholdTimeToPostponeUpdateMs = milliseconds;
        return prevValue;
    }
}
