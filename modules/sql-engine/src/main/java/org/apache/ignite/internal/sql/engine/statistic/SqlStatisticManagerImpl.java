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

/**
 * Statistic manager. Provide and manage update of statistics for SQL.
 */
public class SqlStatisticManagerImpl implements SqlStatisticManager {
    private static final IgniteLogger LOG = Loggers.forClass(SqlStatisticManagerImpl.class);
    private static final long DEFAULT_TABLE_SIZE = 1_000_000;
    private static final long MINIMUM_TABLE_SIZE = 1_000L;

    private static final long THRESHOLD_TIME_TO_POSTPONE_UPDATE_MS = TimeUnit.MINUTES.toMillis(1);

    private final TableManager tableManager;

    private final ConcurrentMap<Integer, Long> tableSizeMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Long> tableSizeUpdateTimeMap = new ConcurrentHashMap<>();


    /** Constructor. */
    public SqlStatisticManagerImpl(TableManager tableManager, CatalogManager catalogManager) {
        this.tableManager = tableManager;

        catalogManager.listen(CatalogEvent.TABLE_DROP, fromConsumer(event -> {
            int tableId = ((DropTableEventParameters) event).tableId();
            onTableDrop(tableId);
        }));
    }

    /**
     * Returns approximate number of rows in table by their id. For first run try to got fresh value, but still can return default table
     * size if can do it in time. Returns the previous known value and can start process to update asked statistics in background to have
     * updated values for future requests.
     */
    @Override
    public long tableSize(int tableId) {
        updateTableSizeStatistics(tableId);

        return Math.max(tableSizeMap.getOrDefault(tableId, DEFAULT_TABLE_SIZE), MINIMUM_TABLE_SIZE);
    }

    /** Update table size statistic in the background if it required. */
    private void updateTableSizeStatistics(int tableId) {
        long lastUpdateTime = tableSizeUpdateTimeMap.getOrDefault(tableId, -1L);
        long currTs = FastTimestamps.coarseCurrentTimeMillis();
        if (currTs >= lastUpdateTime + THRESHOLD_TIME_TO_POSTPONE_UPDATE_MS) {
            TableViewInternal tableView = tableManager.cachedTable(tableId);
            if (tableView == null) {
                LOG.debug("There is no table to update statistics [id={}]", tableId);
                return;
            }
            tableSizeUpdateTimeMap.put(tableId, currTs);

            // just request new table size in background.
            tableView.internalTable().estimatedSize()
                    .thenApply(size -> {
                        Long prevVal = tableSizeMap.put(tableId, size);
                        if (prevVal == null && lastUpdateTime != -1) {
                            // the table was concurrently dropped and we need to remove cached data for them.
                            onTableDrop(tableId);
                        }
                        return prevVal;
                    }).exceptionally(e -> {
                        LOG.info("Can't calculate size for table [id={}]", e, tableId);
                        return DEFAULT_TABLE_SIZE;
                    });
        }
    }

    /** Cleans up resources after a table is dropped. */
    private void onTableDrop(int tableId) {
        tableSizeUpdateTimeMap.remove(tableId);
        tableSizeMap.remove(tableId);
    }
}
