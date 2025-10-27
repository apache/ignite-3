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
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent;
import org.apache.ignite.internal.table.LongPriorityQueue;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.FastTimestamps;
import org.jetbrains.annotations.TestOnly;

/**
 * Statistic manager. Provide and manage update of statistics for SQL.
 */
public class SqlStatisticManagerImpl implements SqlStatisticUpdateManager {
    private static final IgniteLogger LOG = Loggers.forClass(SqlStatisticManagerImpl.class);
    static final long DEFAULT_TABLE_SIZE = 1L;
    private static final ActualSize DEFAULT_VALUE = new ActualSize(DEFAULT_TABLE_SIZE, 0L);

    private final EventListener<ChangeLowWatermarkEventParameters> lwmListener = fromConsumer(this::onLwmChanged);
    private final EventListener<DropTableEventParameters> dropTableEventListener = fromConsumer(this::onTableDrop);
    private final EventListener<CreateTableEventParameters> createTableEventListener = fromConsumer(this::onTableCreate);

    private final AtomicReference<CompletableFuture<Void>> latestUpdateFut = new AtomicReference<>(nullCompletedFuture());

    /** A queue for deferred table destruction events. */
    private final LongPriorityQueue<DestroyTableEvent> destructionEventsQueue = new LongPriorityQueue<>(DestroyTableEvent::catalogVersion);

    private final TableManager tableManager;
    private final CatalogService catalogService;
    private final LowWatermark lowWatermark;

    private final AtomicReference<StatisticUpdatesSupplier> changesSupplier = new AtomicReference<>();

    /* Contains all known table id's with statistics. */
    private final ConcurrentMap<Integer, ActualSize> tableSizeMap = new ConcurrentHashMap<>();

    private volatile long thresholdTimeToPostponeUpdateMs = TimeUnit.MINUTES.toMillis(1);

    /** Constructor. */
    public SqlStatisticManagerImpl(TableManager tableManager, CatalogService catalogService, LowWatermark lowWatermark) {
        this.tableManager = tableManager;
        this.catalogService = catalogService;
        this.lowWatermark = lowWatermark;
    }

    @Override
    public void changesNotifier(StatisticUpdatesSupplier updater) {
        if (!this.changesSupplier.compareAndSet(null, updater)) {
            throw new AssertionError("Statistics updater unexpected change");
        }
    }

    /**
     * Returns approximate number of rows in table by their id.
     *
     * <p>Returns the previous known value or {@value SqlStatisticManagerImpl#DEFAULT_TABLE_SIZE} as default value. Can start process to
     * update asked statistics in background to have updated values for future requests.
     *
     * @return An approximate number of rows in a given table.
     */
    @Override
    public long tableSize(int tableId) {
        updateTableSizeStatistics(tableId, false);

        return tableSizeMap.getOrDefault(tableId, DEFAULT_VALUE).getSize();
    }

    /** Update table size statistic in the background if it required. */
    private void updateTableSizeStatistics(int tableId, boolean force) {
        TableViewInternal tableView = tableManager.cachedTable(tableId);
        if (tableView == null) {
            LOG.debug("There is no table to update statistics [id={}].", tableId);
            return;
        }

        ActualSize tableSize = tableSizeMap.get(tableId);
        if (tableSize == null) {
            // has been concurrently cleaned up, no need more update statistic for the table.
            return;
        }

        long currTimestamp = FastTimestamps.coarseCurrentTimeMillis();
        long lastUpdateTime = tableSize.getTimestamp();

        if (force || lastUpdateTime <= currTimestamp - thresholdTimeToPostponeUpdateMs) {
            // Prevent to run update for the same table twice concurrently.
            if (!force && !tableSizeMap.replace(tableId, tableSize, new ActualSize(tableSize.getSize(), currTimestamp - 1))) {
                return;
            }

            // just request new table size in background.
            CompletableFuture<Void> updateResult = tableView.internalTable().estimatedSize()
                    .thenAccept(size -> {
                        // the table can be concurrently dropped and we shouldn't put new value in this case.
                        tableSizeMap.computeIfPresent(tableId, (k, v) -> {
                            // Discard current computation if value in cache is newer than current one.
                            if (v.timestamp >= currTimestamp) {
                                return v;
                            }

                            return new ActualSize(Math.max(size, 1), currTimestamp);
                        });
                    }).handle((res, err) -> {
                        if (err != null) {
                            LOG.warn(format("Can't calculate size for table [id={}].", tableId), err);

                            return null;
                        } else {
                            StatisticUpdatesSupplier supplier = changesSupplier.get();
                            if (supplier != null) {
                                supplier.accept(tableId);
                            }

                            return res;
                        }
                    });

            latestUpdateFut.updateAndGet(prev -> prev == null ? updateResult : prev.thenCompose(none -> updateResult));
        }
    }

    @Override
    public void start() {
        catalogService.listen(CatalogEvent.TABLE_CREATE, createTableEventListener);
        catalogService.listen(CatalogEvent.TABLE_DROP, dropTableEventListener);
        lowWatermark.listen(LowWatermarkEvent.LOW_WATERMARK_CHANGED, lwmListener);

        // Need to have all known tables for all available history of catalog.
        int earliestVersion = catalogService.earliestCatalogVersion();
        int latestVersion = catalogService.latestCatalogVersion();
        for (int version = earliestVersion; version <= latestVersion; version++) {
            Collection<CatalogTableDescriptor> tables = catalogService.catalog(version).tables();
            for (CatalogTableDescriptor table : tables) {
                tableSizeMap.putIfAbsent(table.id(), DEFAULT_VALUE);
            }
        }
    }

    @Override
    public void stop() {
        lowWatermark.removeListener(LowWatermarkEvent.LOW_WATERMARK_CHANGED, lwmListener);
        catalogService.removeListener(CatalogEvent.TABLE_DROP, dropTableEventListener);
        catalogService.removeListener(CatalogEvent.TABLE_CREATE, createTableEventListener);
    }

    private void onTableDrop(DropTableEventParameters parameters) {
        int tableId = parameters.tableId();
        int catalogVersion = parameters.catalogVersion();

        destructionEventsQueue.enqueue(new DestroyTableEvent(catalogVersion, tableId));
    }

    private void onTableCreate(CreateTableEventParameters parameters) {
        tableSizeMap.put(parameters.tableId(), DEFAULT_VALUE);
    }

    private void onLwmChanged(ChangeLowWatermarkEventParameters parameters) {
        int earliestVersion = catalogService.activeCatalogVersion(parameters.newLowWatermark().longValue());
        List<DestroyTableEvent> events = destructionEventsQueue.drainUpTo(earliestVersion);

        events.forEach(event -> tableSizeMap.remove(event.tableId()));
    }

    /** Timestamped size. */
    private static class ActualSize {
        long timestamp;
        long size;

        ActualSize(long size, long timestamp) {
            this.timestamp = timestamp;
            this.size = size;
        }

        long getTimestamp() {
            return timestamp;
        }

        long getSize() {
            return size;
        }
    }

    /** Internal event. */
    private static class DestroyTableEvent {
        final int catalogVersion;
        final int tableId;

        DestroyTableEvent(int catalogVersion, int tableId) {
            this.catalogVersion = catalogVersion;
            this.tableId = tableId;
        }

        public int catalogVersion() {
            return catalogVersion;
        }

        public int tableId() {
            return tableId;
        }
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

    /**
     * Returns feature for the last run update statistics to have ability wait update statistics.
     */
    @TestOnly
    public CompletableFuture<Void> lastUpdateStatisticFuture() {
        return latestUpdateFut.get();
    }

    /** Forcibly updates statistics for all known tables, ignoring throttling. */
    @TestOnly
    public void forceUpdateAll() {
        List<Integer> tableIds = List.copyOf(tableSizeMap.keySet());

        for (int tableId : tableIds) {
            updateTableSizeStatistics(tableId, true);
        }
    }
}
