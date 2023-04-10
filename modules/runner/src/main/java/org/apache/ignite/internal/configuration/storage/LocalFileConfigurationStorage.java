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

package org.apache.ignite.internal.configuration.storage;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationFlattener.createFlattenedUpdatesMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.addDefaults;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.fillFromPrefixMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.internalSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.toPrefixMap;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.impl.ConfigImpl;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.configuration.NodeConfigCreateException;
import org.apache.ignite.internal.configuration.NodeConfigWriteException;
import org.apache.ignite.internal.configuration.RootInnerNode;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.tree.ConverterToMapVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation of {@link ConfigurationStorage} based on local file configuration storage.
 */
public class LocalFileConfigurationStorage implements ConfigurationStorage {
    private static final IgniteLogger LOG = Loggers.forClass(LocalFileConfigurationStorage.class);

    /** Path to config file. */
    private final Path configPath;

    /** Path to temporary configuration storage. */
    private final Path tempConfigPath;

    /** R/W lock to guard the latest configuration and config file. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Latest state of last applied configuration. */
    private final Map<String, Serializable> latest = new ConcurrentHashMap<>();

    /** Configuration nodes generator. */
    private final ConfigurationAsmGenerator cgen = new ConfigurationAsmGenerator();

    /** Map of root keys that are needed to generate configuration tree. */
    private final Map<String, RootKey<?, ?>> rootKeys;

    /** Configuration changes listener. */
    private final AtomicReference<ConfigurationStorageListener> lsnrRef = new AtomicReference<>();

    /** Thread pool for configuration updates notifications. */
    private final ExecutorService threadPool = Executors.newFixedThreadPool(
            2, new NamedThreadFactory("loc-cfg-file", LOG)
    );

    /** Thread pool for configuration updates notifications. */
    private final ExecutorService workerThreadPool = Executors.newFixedThreadPool(
            2, new NamedThreadFactory("cfg-file-worker", LOG)
    );

    /** Tracks all running futures. */
    private final InFlightFutures futureTracker = new InFlightFutures();

    /** Last revision for configuration. */
    private long lastRevision = 0L;

    /**
     * Constructor without configuration extensions.
     *
     * @param configPath Path to node bootstrap configuration file.
     * @param rootKeys Configuration root keys.
     */
    public LocalFileConfigurationStorage(Path configPath, Collection<RootKey<?, ?>> rootKeys) {
        this(configPath, rootKeys, Collections.emptyList(), Collections.emptyList());
    }

    /**
     * Constructor.
     *
     * @param configPath Path to node bootstrap configuration file.
     * @param rootKeys Configuration root keys.
     * @param internalSchemaExtensions Internal schema extensions.
     * @param polymorphicSchemaExtensions Polymorphic schema extensions.
     */
    public LocalFileConfigurationStorage(Path configPath, Collection<RootKey<?, ?>> rootKeys,
            Collection<Class<?>> internalSchemaExtensions, Collection<Class<?>> polymorphicSchemaExtensions) {
        this.configPath = configPath;
        this.rootKeys = rootKeys.stream().collect(toMap(RootKey::key, identity()));
        this.tempConfigPath = configPath.resolveSibling(configPath.getFileName() + ".tmp");

        Map<Class<?>, Set<Class<?>>> internalExtensions = internalSchemaExtensions(internalSchemaExtensions);
        Map<Class<?>, Set<Class<?>>> polymorphicExtensions = polymorphicSchemaExtensions(polymorphicSchemaExtensions);

        rootKeys.forEach(key -> cgen.compileRootSchema(key.schemaClass(), internalExtensions, polymorphicExtensions));

        checkAndRestoreConfigFile();
    }

    private static void setValues(SuperRoot target, Config source) {
        HoconConverter.hoconSource(source.root()).descend(target);
    }

    @Override
    public CompletableFuture<Data> readDataOnRecovery() {
        return writeLockAsync(() -> {
            SuperRoot superRoot = createSuperRoot();
            SuperRoot copiedSuperRoot = superRoot.copy();

            Config hocon = readHoconFromFile();
            setValues(copiedSuperRoot, hocon);

            addDefaults(copiedSuperRoot);

            Map<String, Serializable> flattenedUpdatesMap = createFlattenedUpdatesMap(superRoot, copiedSuperRoot);

            latest.putAll(flattenedUpdatesMap);

            return new Data(flattenedUpdatesMap, lastRevision);
        });
    }


    private Config readHoconFromFile() {
        checkAndRestoreConfigFile();
        return ConfigFactory.parseFile(configPath.toFile(), ConfigParseOptions.defaults().setAllowMissing(false));
    }

    @NotNull
    private SuperRoot createSuperRoot() {
        SuperRoot superRoot = new SuperRoot(rootCreator());
        for (RootKey<?, ?> rootKey : rootKeys.values()) {
            superRoot.addRoot(rootKey, createRootNode(rootKey));
        }

        return superRoot;
    }

    @Override
    public CompletableFuture<Map<String, ? extends Serializable>> readAllLatest(String prefix) {
        return readLockAsync(() ->
                latest.entrySet()
                        .stream()
                        .filter(entry -> entry.getKey().startsWith(prefix))
                        .collect(toMap(Entry::getKey, Entry::getValue))
        );
    }

    @Override
    public CompletableFuture<Serializable> readLatest(String key) {
        return readLockAsync(() -> latest.get(key));
    }

    @Override
    public CompletableFuture<Boolean> write(Map<String, ? extends Serializable> newValues, long ver) {
        return writeLockAsync(() -> {
            if (ver != lastRevision) {
                return false;
            }

            mergeAndSave(newValues);

            runAsync(() -> lsnrRef.get().onEntriesChanged(new Data(newValues, lastRevision)));

            return true;
        });
    }

    private void mergeAndSave(Map<String, ? extends Serializable> newValues) {
        updateLatestState(newValues);
        saveConfigFile();
        lastRevision++;
    }

    private void updateLatestState(Map<String, ? extends Serializable> newValues) {
        newValues.forEach((key, value) -> {
            if (value == null) { // null means that we should remove this entry
                latest.remove(key);
            } else {
                latest.put(key, value);
            }
        });
    }

    private void runAsync(Runnable runnable) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(runnable, threadPool);
        futureTracker.registerFuture(future);
    }

    @Override
    public void registerConfigurationListener(ConfigurationStorageListener lsnr) {
        if (!lsnrRef.compareAndSet(null, lsnr)) {
            LOG.debug("Configuration listener has already been set");
        }
    }

    @Override
    public ConfigurationType type() {
        return ConfigurationType.LOCAL;
    }

    @Override
    public CompletableFuture<Long> lastRevision() {
        return CompletableFuture.completedFuture(lastRevision);
    }

    @Override
    public CompletableFuture<Void> writeConfigurationRevision(long prevRevision, long currentRevision) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
        IgniteUtils.shutdownAndAwaitTermination(workerThreadPool, 10, TimeUnit.SECONDS);
        IgniteUtils.shutdownAndAwaitTermination(threadPool, 10, TimeUnit.SECONDS);

        futureTracker.cancelInFlightFutures();
    }

    private void saveConfigFile() {
        try {
            Files.write(
                    tempConfigPath,
                    renderHoconString().getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.SYNC, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
            );

            Files.move(
                    tempConfigPath,
                    configPath,
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING
            );
        } catch (IOException e) {
            throw new NodeConfigWriteException(
                    "Failed to write values to config file.", e);
        }
    }

    /**
     * Convert provided map to Hocon String representation.
     *
     * @return Configuration file string representation in HOCON format.
     */
    private String renderHoconString() {
        // Super root that'll be filled from the storage data.
        SuperRoot rootNode = new SuperRoot(rootCreator());

        fillFromPrefixMap(rootNode, toPrefixMap(latest));

        addDefaults(rootNode);

        ConfigValue conf = ConfigImpl.fromAnyRef(HoconConverter.represent(rootNode, new ConverterToMapVisitor(false)), null);

        Config newConfig = ((ConfigObject) conf).toConfig().resolve();
        return newConfig.isEmpty()
                ? ""
                : newConfig.root().render(ConfigRenderOptions.concise().setFormatted(true).setJson(false));
    }

    private Function<String, RootInnerNode> rootCreator() {
        return key -> {
            RootKey<?, ?> rootKey = rootKeys.get(key);

            return rootKey == null ? null : new RootInnerNode(rootKey, createRootNode(rootKey));
        };
    }

    private InnerNode createRootNode(RootKey<?, ?> rootKey) {
        return cgen.instantiateNode(rootKey.schemaClass());
    }

    private Config parseConfigOptions() {
        return readHoconFromFile();
    }

    /** Check that configuration file still exists and restore it with latest applied state in case it was deleted. */
    private void checkAndRestoreConfigFile() {
        if (!configPath.toFile().exists()) {
            try {
                if (configPath.toFile().createNewFile()) {
                    if (!latest.isEmpty()) {
                        saveConfigFile();
                    }
                } else {
                    throw new NodeConfigCreateException("Failed to re-create config file");
                }
            } catch (IOException e) {
                throw new NodeConfigWriteException("Failed to restore config file.", e);
            }
        }
    }

    private <T> CompletableFuture<T> readLockAsync(Supplier<T> supplier) {
        lock.readLock().lock();
        try {
            CompletableFuture<T> future = CompletableFuture.supplyAsync(supplier, workerThreadPool);
            futureTracker.registerFuture(future);
            return future;
        } finally {
            lock.readLock().unlock();
        }
    }

    private <T> CompletableFuture<T> writeLockAsync(Supplier<T> supplier) {
        lock.writeLock().lock();
        try {
            CompletableFuture<T> future = CompletableFuture.supplyAsync(supplier, workerThreadPool);
            futureTracker.registerFuture(future);
            return future;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
