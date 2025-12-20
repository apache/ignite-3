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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.sql.engine.statistic.event.StatisticChangedEvent.STATISTIC_CHANGED;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent;
import org.apache.ignite.internal.sql.engine.statistic.event.StatisticChangedEvent;
import org.apache.ignite.internal.sql.engine.statistic.event.StatisticEventParameters;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.LongPriorityQueue;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.jetbrains.annotations.TestOnly;

/**
 * Statistic manager. Provide and manage update of statistics for SQL.
 */
public class SqlStatisticManagerImpl extends AbstractEventProducer<StatisticChangedEvent, StatisticEventParameters>
        implements SqlStatisticUpdateManager {
    private static final IgniteLogger LOG = Loggers.forClass(SqlStatisticManagerImpl.class);
    static final long DEFAULT_TABLE_SIZE = 1L;
    private static final ActualSize DEFAULT_VALUE = new ActualSize(DEFAULT_TABLE_SIZE, Long.MIN_VALUE);

    private final EventListener<ChangeLowWatermarkEventParameters> lwmListener = fromConsumer(this::onLwmChanged);
    private final EventListener<DropTableEventParameters> dropTableEventListener = fromConsumer(this::onTableDrop);
    private final EventListener<CreateTableEventParameters> createTableEventListener = fromConsumer(this::onTableCreate);

    private final AtomicReference<CompletableFuture<Void>> latestUpdateFut = new AtomicReference<>(nullCompletedFuture());

    /** A queue for deferred table destruction events. */
    private final LongPriorityQueue<DestroyTableEvent> destructionEventsQueue = new LongPriorityQueue<>(DestroyTableEvent::catalogVersion);

    private final TableManager tableManager;
    private final CatalogService catalogService;
    private final LowWatermark lowWatermark;

    /* Contains all known table id's with statistics. */
    final ConcurrentMap<Integer, ActualSize> tableSizeMap = new ConcurrentHashMap<>();

    /* Contain dropped tables, can`t update statistic for such a case. */
    final Set<Integer> droppedTables = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final ScheduledExecutorService scheduler;
    private final StatisticAggregator<Collection<InternalTable>, CompletableFuture<Int2ObjectMap<PartitionModificationInfo>>> statSupplier;

    private final ConfigurationValue<Integer> staleRowsCheckIntervalSeconds;

    private final ConfigurationListener<Integer> updateRefreshIntervalListener = this::updateConfig;

    private volatile ScheduledFuture<?> scheduledFuture;

    /** Constructor. */
    public SqlStatisticManagerImpl(
            TableManager tableManager,
            CatalogService catalogService,
            LowWatermark lowWatermark,
            ScheduledExecutorService scheduler,
            StatisticAggregator<Collection<InternalTable>, CompletableFuture<Int2ObjectMap<PartitionModificationInfo>>> statSupplier,
            ConfigurationValue<Integer> staleRowsCheckIntervalSeconds
    ) {
        this.tableManager = tableManager;
        this.catalogService = catalogService;
        this.lowWatermark = lowWatermark;
        this.scheduler = scheduler;
        this.statSupplier = statSupplier;
        this.staleRowsCheckIntervalSeconds = staleRowsCheckIntervalSeconds;
    }

    /**
     * Returns approximate number of rows in table.
     *
     * <p>Returns the previous known value or {@value SqlStatisticManagerImpl#DEFAULT_TABLE_SIZE} as default value. Can start process to
     * update asked statistics in background to have updated values for future requests.
     *
     * @return An approximate number of rows in a given table.
     */
    @Override
    public long tableSize(int tableId) {
        return tableSizeMap.getOrDefault(tableId, DEFAULT_VALUE).getSize();
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

        staleRowsCheckIntervalSeconds.listen(updateRefreshIntervalListener);
        int seconds = staleRowsCheckIntervalSeconds.value();

        schedule(0, seconds);
    }

    private CompletableFuture<?> updateConfig(ConfigurationNotificationEvent<Integer> ctx) {
        Integer intervalSeconds = ctx.newValue();
        assert intervalSeconds != null;

        if (!Objects.equals(intervalSeconds, ctx.oldValue())) {
            schedule(intervalSeconds, intervalSeconds);
        }

        return nullCompletedFuture();
    }

    private void schedule(int initialDelaySeconds, int intervalSeconds) {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        scheduledFuture = scheduler.scheduleAtFixedRate(() -> update(false), initialDelaySeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    private void update(boolean force) {
        if (!force && !latestUpdateFut.get().isDone()) {
            return;
        }

        CompletableFuture<Void> currentUpdateResult;
        try {
            currentUpdateResult = doUpdate();
        } catch (Throwable e) {
            currentUpdateResult = failedFuture(e);
        }
        currentUpdateResult.whenComplete((r, t) -> {
            if (t != null) {
                LOG.warn("Failed to update statistics", t);
            }
        });

        CompletableFuture<Void> updateResult = currentUpdateResult;
        latestUpdateFut.updateAndGet(prev -> prev == null ? updateResult : prev.thenCompose(none -> updateResult));
    }

    private CompletableFuture<Void> doUpdate() {
        Collection<InternalTable> tables = new ArrayList<>(tableSizeMap.size());

        for (Map.Entry<Integer, ActualSize> ent : tableSizeMap.entrySet()) {
            Integer tableId = ent.getKey();

            if (droppedTables.contains(tableId)) {
                continue;
            }

            TableViewInternal tableView = tableManager.cachedTable(tableId);

            if (tableView == null) {
                LOG.debug("No table found to update statistics [id={}].", ent.getKey());
            } else {
                tables.add(tableView.internalTable());
            }
        }

        return statSupplier.estimatedSizeWithLastUpdate(tables).thenApply(infos -> {
            for (Int2ObjectMap.Entry<PartitionModificationInfo> ent : infos.int2ObjectEntrySet()) {
                int tableId = ent.getIntKey();
                PartitionModificationInfo info = ent.getValue();

                ActualSize updatedSize = new ActualSize(Math.max(info.getEstimatedSize(), DEFAULT_TABLE_SIZE),
                        info.lastModificationCounter());
                ActualSize currentSize = tableSizeMap.get(tableId);
                // the table can be concurrently dropped and we shouldn't put new value in this case.
                tableSizeMap.compute(tableId, (k, v) -> {
                    return v != null && v.modificationCounter() > info.lastModificationCounter()
                            ? v
                            : updatedSize; // Save initial or replace stale state.
                });

                if (updatedSize.modificationCounter() >= currentSize.modificationCounter()) {
                    fireEvent(STATISTIC_CHANGED, new StatisticEventParameters(tableId));
                }
            }
            return null;
        });
    }

    @Override
    public void stop() {
        lowWatermark.removeListener(LowWatermarkEvent.LOW_WATERMARK_CHANGED, lwmListener);
        catalogService.removeListener(CatalogEvent.TABLE_DROP, dropTableEventListener);
        catalogService.removeListener(CatalogEvent.TABLE_CREATE, createTableEventListener);
        staleRowsCheckIntervalSeconds.stopListen(updateRefreshIntervalListener);

        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    private void onTableDrop(DropTableEventParameters parameters) {
        int tableId = parameters.tableId();
        int catalogVersion = parameters.catalogVersion();

        destructionEventsQueue.enqueue(new DestroyTableEvent(catalogVersion, tableId));
        droppedTables.add(tableId);
    }

    private void onTableCreate(CreateTableEventParameters parameters) {
        tableSizeMap.put(parameters.tableId(), DEFAULT_VALUE);
    }

    private void onLwmChanged(ChangeLowWatermarkEventParameters parameters) {
        int earliestVersion = catalogService.activeCatalogVersion(parameters.newLowWatermark().longValue());
        List<DestroyTableEvent> events = destructionEventsQueue.drainUpTo(earliestVersion);

        events.forEach(event -> tableSizeMap.remove(event.tableId()));
        events.forEach(event -> droppedTables.remove(event.tableId()));
    }

    /** Size with modification counter. */
    static class ActualSize {
        final long modificationCounter;
        final long size;

        ActualSize(long size, long modificationCounter) {
            this.modificationCounter = modificationCounter;
            this.size = size;
        }

        public long modificationCounter() {
            return modificationCounter;
        }

        long getSize() {
            return size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ActualSize that = ((ActualSize) o);

            return modificationCounter == that.modificationCounter && size == that.size;
        }

        @Override
        public int hashCode() {
            return Objects.hash(modificationCounter, size);
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
     * Returns feature for the last run update statistics to have ability wait update statistics.
     */
    @TestOnly
    public CompletableFuture<Void> lastUpdateStatisticFuture() {
        return latestUpdateFut.get();
    }

    /** Forcibly updates statistics for all known tables, ignoring throttling. */
    @TestOnly
    public void forceUpdateAll() {
        update(true);
    }
}
