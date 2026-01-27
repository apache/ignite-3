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

package org.apache.ignite.migrationtools.persistence;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.migrationtools.persistence.exceptions.MigrateCacheException;
import org.apache.ignite.migrationtools.persistence.mappers.CacheDataRowProcessor;
import org.apache.ignite.migrationtools.persistence.utils.pubsub.BasicProcessor;
import org.apache.ignite.migrationtools.persistence.utils.pubsub.StreamerPublisher;
import org.apache.ignite.migrationtools.sql.SqlDdlGenerator;
import org.apache.ignite.migrationtools.tablemanagement.SchemaUtils;
import org.apache.ignite.migrationtools.types.converters.StaticTypeConverterFactory;
import org.apache.ignite.migrationtools.types.converters.TypeConverterFactory;
import org.apache.ignite3.catalog.definitions.TableDefinition;
import org.apache.ignite3.client.IgniteClient;
import org.apache.ignite3.internal.client.table.ClientSchema;
import org.apache.ignite3.internal.client.table.ClientTable;
import org.apache.ignite3.table.DataStreamerItem;
import org.apache.ignite3.table.QualifiedName;
import org.apache.ignite3.table.Tuple;
import org.jetbrains.annotations.Nullable;

/** Utility methods to interact with Ignite 2 persistent caches. */
public class Ignite2PersistentCacheTools {

    /**
     * List the persistent caches in the provided nodes.
     *
     * @param nodes The cluster nodes.
     * @return Set of cacheId cacheName pairs.
     * @throws IgniteCheckedException in case of error.
     */
    public static Set<Pair<Integer, String>> persistentCaches(List<MigrationKernalContext> nodes) throws IgniteCheckedException {
        Set<Pair<Integer, String>> cacheIds = new HashSet<>();
        for (MigrationKernalContext ctx : nodes) {
            var cacheCtx = ctx.cache();
            if (cacheCtx instanceof MigrationCacheProcessor) {
                ((MigrationCacheProcessor) cacheCtx).loadAllDescriptors();
            }

            for (var descr : ctx.cache().persistentCaches()) {
                if (CacheType.USER.equals(descr.cacheType())) {
                    cacheIds.add(Pair.of(descr.cacheId(), descr.cacheName()));
                }
            }
        }
        return cacheIds;
    }

    /**
     * Publishes a cache cursor on the given nodes.
     *
     * @param nodes Cluster nodes.
     * @param cacheName Cache name.
     * @param partitionStreamFactory Function that allows the caller to react when each partition cursor is opened.
     * @throws IgniteCheckedException in case of error.
     */
    public static void publishCacheCursor(
            List<MigrationKernalContext> nodes,
            String cacheName,
            BiFunction<Flow.Publisher<Map.Entry<Object, Object>>, Integer, Optional<CompletableFuture<Void>>> partitionStreamFactory
    ) throws IgniteCheckedException {
        for (MigrationKernalContext nodeCtx : nodes) {
            DynamicCacheDescriptor cacheDescriptor = nodeCtx.cache().cacheDescriptor(cacheName);
            publishCacheCursorAtNode(nodeCtx, cacheDescriptor, partitionStreamFactory);
        }
    }

    /**
     * Publishes a cache cursor on the given node.
     *
     * @param nodeCtx Cluster node context.
     * @param cacheDescriptor Cache descriptor.
     * @param partitionStreamFactory Function that allows the caller to react when each partition cursor is opened.
     * @throws IgniteCheckedException in case of error.
     */
    public static void publishCacheCursorAtNode(
            MigrationKernalContext nodeCtx,
            DynamicCacheDescriptor cacheDescriptor,
            BiFunction<Flow.Publisher<Map.Entry<Object, Object>>, Integer, Optional<CompletableFuture<Void>>> partitionStreamFactory
    ) throws IgniteCheckedException {
        var cacheProcessor = nodeCtx.cache();
        if (cacheProcessor instanceof MigrationCacheProcessor) {
            ((MigrationCacheProcessor) cacheProcessor).startCache(cacheDescriptor);
        }
        int cacheId = cacheDescriptor.cacheId();
        var gridCacheSharedContext = cacheProcessor.context();
        var cacheContext = gridCacheSharedContext.cacheContext(cacheId);
        var offHeap = cacheContext.offheap();
        var cacheObjectCtx = cacheContext.cacheObjectContext();

        for (IgniteCacheOffheapManager.CacheDataStore cs : offHeap.cacheDataStores()) {
            StreamerPublisher<CacheDataRow> cacheTuplesPublisher = new StreamerPublisher<>();

            CacheDataRowProcessor rowProc = new CacheDataRowProcessor(cacheObjectCtx);
            cacheTuplesPublisher.subscribe(rowProc);

            var streamJobOpt = partitionStreamFactory.apply(rowProc, cs.partId());
            if (streamJobOpt.isPresent()) {
                CompletableFuture<Void> streamJob = streamJobOpt.get();
                try {
                    // TODO: IGNITE-27629 Allow more control on the converters side.
                    GridCursor<? extends CacheDataRow> cu = cs.cursor();

                    while (cu.next()) {
                        CacheDataRow row = cu.get();
                        // Currently, I don't know how to check this more efficiently.
                        // Skips other caches in the same grp. There might be a better way to do this.
                        // Caches outside groups may have the cacheId set to 0
                        if (row.cacheId() != 0 && row.cacheId() != cacheId) {
                            continue;
                        }

                        try {
                            boolean success = cacheTuplesPublisher.offer(row);
                            if (!success) {
                                throw new IgniteCheckedException("Failed to offer element to publisher");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IgniteCheckedException("Interrupted publishing tuple");
                        }
                    }
                } catch (IgniteCheckedException ex) {
                    cacheTuplesPublisher.closeExceptionally(ex);
                    throw ex;
                } finally {
                    cacheTuplesPublisher.close();
                    streamJob.join();
                }
            }
        }
    }

    /**
     * Wrapper method that migrates a given cache.
     *
     * @param client Adapted client.
     * @param sqlGenerator SQL DDL Generator.
     * @param nodeContexts Node contexts.
     * @param cacheName Cache name.
     * @param columnsProcessorFactory Column processor factory.
     * @throws IgniteCheckedException in case of error.
     */
    public static void migrateCache(
            IgniteClient client,
            SqlDdlGenerator sqlGenerator,
            List<MigrationKernalContext> nodeContexts,
            String cacheName,
            ColumnsProcessorFactory columnsProcessorFactory
    ) throws IgniteCheckedException {
        CacheConfiguration<?, ?> cacheCfg = nodeContexts.stream()
                .flatMap(nodeCtx -> Stream.ofNullable(nodeCtx.cache().cacheDescriptor(cacheName)))
                .map(DynamicCacheDescriptor::cacheConfiguration)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Could not find the requested cache: " + cacheName));

        // TODO: IGNITE-27629 Allow injecting custom aliases/fieldNameForColumn mappings
        QualifiedName qualifiedName = SqlDdlGenerator.qualifiedName(cacheCfg);
        @Nullable ClientTable table = (ClientTable) client.tables().table(qualifiedName);
        SqlDdlGenerator.GenerateTableResult tableDefinition = sqlGenerator.generate(cacheCfg);
        Map<String, String> columnToFieldMappings = tableDefinition.fieldToColumnMappings();
        if (table == null) {
            TableDefinition tblDef = tableDefinition.tableDefinition();
            if (!"PUBLIC".equals(tblDef.schemaName())) {
                client.sql().executeAsync(null, "CREATE SCHEMA IF NOT EXISTS " + tblDef.schemaName() + ";").join();
            }

            table = (ClientTable) client.catalog()
                    .createTableAsync(tblDef)
                    .join();
        }

        var view = table.keyValueView();
        ClientSchema schema = SchemaUtils.getLatestSchemaForTable(table).join();
        String tableName = table.name();

        // TODO: IGNITE-27629 Allow more control on the converters side.
        // Call dump table
        publishCacheCursor(nodeContexts, cacheName, (cacheTuplesPublisher, partId) ->
                Optional.ofNullable(columnsProcessorFactory.createSubscribed(
                                cacheTuplesPublisher,
                                partId,
                                schema,
                                columnToFieldMappings,
                                StaticTypeConverterFactory.DEFAULT_INSTANCE))
                        .map(itemPublisher -> {
                                var p = new BasicProcessor<DataStreamerItem<Map.Entry<Tuple, Tuple>>,
                                        DataStreamerItem<Map.Entry<Tuple, Tuple>>>() {
                                    @Override
                                    public void onNext(DataStreamerItem<Entry<Tuple, Tuple>> item) {
                                        subscriber.onNext(item);
                                    }

                                    @Override
                                    public void onError(Throwable throwable) {
                                        super.onError(new MigrateCacheException(cacheName, tableName, throwable));
                                    }
                                };

                                itemPublisher.subscribe(p);
                                return p;
                            }
                        )
                        .map((itemPublisher) -> view.streamData(itemPublisher, null)));
    }

    /** ColumnsProcessorFactory. */
    public interface ColumnsProcessorFactory {
        @Nullable
        Flow.Publisher<DataStreamerItem<Map.Entry<Tuple, Tuple>>> createSubscribed(
                Flow.Publisher<Map.Entry<Object, Object>> cacheTuplePublisher,
                int partitionId,
                ClientSchema clientSchema,
                Map<String, String> columnsToFieldsMappings,
                TypeConverterFactory typeConverters);
    }
}
