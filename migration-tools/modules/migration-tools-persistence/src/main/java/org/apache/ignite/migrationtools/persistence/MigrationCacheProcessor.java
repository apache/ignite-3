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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheConfigurationEnrichment;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.CacheJoinNodeDiscoveryData;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.query.QuerySchema;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteUuid;

/**
 * This is a lazy version of the {@link GridCacheProcessor}.
 *
 * <p>This class does not start all the caches automatically during startup.
 * Instead, it blocks the CacheRecoveryLifecycle of the underlying {@link GridCacheProcessor}.
 * {@link DynamicCacheDescriptor}s can be explicitly loaded using {@link #cacheDescriptor(String)} or {@link #loadAllDescriptors()}.
 * Caches must be started explicitly using {@link #startCache(DynamicCacheDescriptor)}.
 *
 * <p>Important: This is not a fully featured {@link GridCacheProcessor}, it should only be used with caution in the context of a migration.
 */
public class MigrationCacheProcessor extends GridCacheProcessor {

    private static final Constructor<CacheGroupDescriptor> CACHE_GROUP_DESCRIPTOR_CONSTRUCTOR;

    static {
        try {
            CACHE_GROUP_DESCRIPTOR_CONSTRUCTOR = CacheGroupDescriptor.class.getDeclaredConstructor(
                    CacheConfiguration.class,
                    String.class,
                    int.class,
                    UUID.class,
                    AffinityTopologyVersion.class,
                    IgniteUuid.class,
                    Map.class,
                    boolean.class,
                    boolean.class,
                    Collection.class,
                    CacheConfigurationEnrichment.class
            );
            CACHE_GROUP_DESCRIPTOR_CONSTRUCTOR.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Method START_CACHE_IN_RECOVERY_METHOD;

    static {
        try {
            START_CACHE_IN_RECOVERY_METHOD = GridCacheProcessor.class.getDeclaredMethod(
                    "startCacheInRecoveryMode",
                    DynamicCacheDescriptor.class
            );
            START_CACHE_IN_RECOVERY_METHOD.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Field RECOVERY_LISTENER_FIELD;

    private static final Method RESTORE_PARTITION_STATE_METHOD;

    static {
        try {
            RECOVERY_LISTENER_FIELD = GridCacheProcessor.class.getDeclaredField("recovery");
            RECOVERY_LISTENER_FIELD.setAccessible(true);

            RESTORE_PARTITION_STATE_METHOD = RECOVERY_LISTENER_FIELD.getType().getDeclaredMethod(
                    "restorePartitionStates",
                    Collection.class,
                    Map.class
            );
            RESTORE_PARTITION_STATE_METHOD.setAccessible(true);
        } catch (NoSuchMethodException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private final Object recoveryField;

    private final Map<String, DynamicCacheDescriptor> cacheDescriptors;

    private FilePageStoreManager pageStoreManager;

    /** Constructor. */
    public MigrationCacheProcessor(GridKernalContext ctx) {
        super(ctx);
        this.cacheDescriptors = new HashMap<>();

        try {
            recoveryField = RECOVERY_LISTENER_FIELD.get(this);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start() throws IgniteCheckedException {
        super.start();
        this.pageStoreManager = (FilePageStoreManager) this.context().pageStore();
    }

    private DynamicCacheDescriptor computeDescriptor(String name) {
        var cacheData = readStoredCacheData(name);
        return computeDescriptor(cacheData);
    }

    private DynamicCacheDescriptor computeDescriptor(StoredCacheData cacheData) {
        String name = cacheData.config().getName();
        CacheType cacheType = ctx.cache().cacheType(name);
        var cacheJoinInfo = new CacheJoinNodeDiscoveryData.CacheInfo(cacheData, cacheType, cacheData.sql(), 1, false);

        CacheJoinNodeDiscoveryData discoData = new CacheJoinNodeDiscoveryData(
                IgniteUuid.randomUuid(),
                Collections.singletonMap(name, cacheJoinInfo),
                Collections.emptyMap(),
                false
        );

        return registerNewCache(discoData, ctx.localNodeId(), cacheJoinInfo);
    }

    @Override
    public DynamicCacheDescriptor cacheDescriptor(String name) {
        // TODO: Check if this should be made thread-safe
        return this.cacheDescriptors.computeIfAbsent(name, this::computeDescriptor);
    }

    @Override
    public Map<String, DynamicCacheDescriptor> cacheDescriptors() {
        return this.cacheDescriptors;
    }

    @Override
    public Collection<DynamicCacheDescriptor> persistentCaches() {
        // Assumes all the caches are persistent
        return Collections.unmodifiableCollection(this.cacheDescriptors.values());
    }

    /**
     * Load all descriptors.
     *
     * @throws IgniteCheckedException in case of an error.
     */
    public void loadAllDescriptors() throws IgniteCheckedException {
        Map<String, StoredCacheData> cacheData = this.configManager().readCacheConfigurations();
        for (Map.Entry<String, StoredCacheData> entry : cacheData.entrySet()) {
            this.cacheDescriptors.computeIfAbsent(entry.getKey(), key -> this.computeDescriptor(entry.getValue()));
        }
    }

    /**
     * Starts a cache.
     *
     * @param cacheDescr The descriptor of the cache to be started.
     */
    public void startCache(DynamicCacheDescriptor cacheDescr) {
        try {
            GridCacheContext<?, ?> cacheCtx = (GridCacheContext<?, ?>) START_CACHE_IN_RECOVERY_METHOD.invoke(this, cacheDescr);

            // Restore partition states
            RESTORE_PARTITION_STATE_METHOD.invoke(this.recoveryField, Collections.singleton(cacheCtx.group()), Collections.emptyMap());
        } catch (Exception ex) {
            throw new RuntimeException("Could not start cache: " + cacheDescr, ex);
        }
    }

    private DynamicCacheDescriptor registerNewCache(
            CacheJoinNodeDiscoveryData joinData,
            UUID nodeId,
            CacheJoinNodeDiscoveryData.CacheInfo cacheInfo
    ) {
        // Adapted from org.apache.ignite.internal.processors.cache.ClusterCachesInfo.registerNewCache
        CacheConfiguration<?, ?> cfg = cacheInfo.cacheData().config();

        int cacheId = CU.cacheId(cfg.getName());
        int grpId = CU.cacheGroupId(cfg.getName(), cfg.getGroupName());
        Map<String, Integer> caches = Collections.singletonMap(cfg.getName(), cacheId);

        boolean persistent = true;
        // client nodes cannot read wal enabled/disabled status so they should use default one
        boolean walGloballyEnabled = (ctx.clientNode())
                ? persistent
                : ctx.cache().context().database().walEnabled(grpId, false);

        CacheGroupDescriptor grpDesc;
        try {
            grpDesc = CACHE_GROUP_DESCRIPTOR_CONSTRUCTOR.newInstance(
                    cfg,
                    cfg.getGroupName(),
                    grpId,
                    nodeId,
                    null,
                    joinData.cacheDeploymentId(),
                    caches,
                    persistent,
                    walGloballyEnabled,
                    null,
                    cacheInfo.cacheData().cacheConfigurationEnrichment()
            );
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        return new DynamicCacheDescriptor(ctx,
                cfg,
                cacheInfo.cacheType(),
                grpDesc,
                false,
                nodeId,
                cacheInfo.isStaticallyConfigured(),
                cacheInfo.sql(),
                joinData.cacheDeploymentId(),
                new QuerySchema(cacheInfo.cacheData().queryEntities()),
                cacheInfo.cacheData().cacheConfigurationEnrichment()
        );
    }

    private StoredCacheData readStoredCacheData(String cacheName) {
        // Adapted from FilePageStoreManager
        File storeWorkDir = pageStoreManager.workDir();
        Path workDirPath = storeWorkDir.toPath();

        // Direct cache folder
        String folderName = FilePageStoreManager.CACHE_DIR_PREFIX + cacheName;
        Path directCfgPath = workDirPath.resolve(folderName).resolve(FilePageStoreManager.CACHE_DATA_FILENAME);

        Stream<Path> grpCandidates = null;
        try {
            String groupFileName = cacheName + FilePageStoreManager.CACHE_DATA_FILENAME;
            grpCandidates = Files.list(workDirPath)
                    .filter(path -> path.getFileName().toString().startsWith(FilePageStoreManager.CACHE_GRP_DIR_PREFIX))
                    .map(path -> path.resolve(groupFileName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        var marshaller = ctx.marshallerContext().jdkMarshaller();
        var cfg = ctx.config();
        return Stream.concat(Stream.of(directCfgPath), grpCandidates)
                .filter(path -> Files.exists(path) && !Files.isDirectory(path))
                .map(Path::toFile)
                .map(f -> {
                    try {
                        return configManager().readCacheData(f, marshaller, cfg);
                    } catch (IgniteCheckedException e) {
                        throw new RuntimeException(e);
                    }
                })
                .findFirst()
                .orElseThrow();
    }
}
