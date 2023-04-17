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

import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationFlattener.createFlattenedUpdatesMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.addDefaults;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.fillFromPrefixMap;
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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.NodeConfigCreateException;
import org.apache.ignite.internal.configuration.NodeConfigWriteException;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.tree.ConverterToMapVisitor;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;

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

    /** Configuration tree generator. */
    private final ConfigurationTreeGenerator generator;

    /** Configuration changes listener. */
    private final AtomicReference<ConfigurationStorageListener> lsnrRef = new AtomicReference<>();

    /** Thread pool for configuration updates notifications. */
    private final ExecutorService notificationsThreadPool = Executors.newFixedThreadPool(
            2, new NamedThreadFactory("cfg-file", LOG)
    );

    /** Thread pool for configuration updates. */
    private final ExecutorService workerThreadPool = Executors.newFixedThreadPool(
            2, new NamedThreadFactory("cfg-file-worker", LOG)
    );

    /** Tracks all running futures. */
    private final InFlightFutures futureTracker = new InFlightFutures();

    /** Last revision for configuration. */
    private long lastRevision = 0L;

    /**
     * Constructor.
     *
     * @param configPath Path to node bootstrap configuration file.
     * @param generator Configuration tree generator.
     */
    public LocalFileConfigurationStorage(Path configPath, ConfigurationTreeGenerator generator) {
        this.configPath = configPath;
        this.generator = generator;
        this.tempConfigPath = configPath.resolveSibling(configPath.getFileName() + ".tmp");

        checkAndRestoreConfigFile();
    }

    @Override
    public CompletableFuture<Data> readDataOnRecovery() {
        return async(() -> {
            lock.writeLock().lock();
            try {
                SuperRoot superRoot = generator.createSuperRoot();
                SuperRoot copiedSuperRoot = superRoot.copy();

                Config hocon = readHoconFromFile();
                HoconConverter.hoconSource(hocon.root()).descend(copiedSuperRoot);

                Map<String, Serializable> flattenedUpdatesMap = createFlattenedUpdatesMap(superRoot, copiedSuperRoot);

                latest.putAll(flattenedUpdatesMap);

                return new Data(flattenedUpdatesMap, lastRevision);
            } finally {
                lock.writeLock().unlock();
            }
        });
    }

    private Config readHoconFromFile() {
        checkAndRestoreConfigFile();

        return ConfigFactory.parseFile(configPath.toFile(), ConfigParseOptions.defaults().setAllowMissing(false));
    }

    @Override
    public CompletableFuture<Map<String, ? extends Serializable>> readAllLatest(String prefix) {
        return async(() -> {
            lock.readLock().lock();
            try {
                return latest.entrySet()
                        .stream()
                        .filter(entry -> entry.getKey().startsWith(prefix))
                        .collect(toMap(Entry::getKey, Entry::getValue));
            } finally {
                lock.readLock().unlock();
            }
        });
    }

    @Override
    public CompletableFuture<Serializable> readLatest(String key) {
        return async(() -> {
            lock.readLock().lock();
            try {
                return latest.get(key);
            } finally {
                lock.readLock().unlock();
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> write(Map<String, ? extends Serializable> newValues, long ver) {
        return async(() -> {
            lock.writeLock().lock();
            try {
                if (ver != lastRevision) {
                    return false;
                }

                mergeAndSave(newValues);

                sendNotificationAsync(new Data(newValues, lastRevision));

                return true;
            } finally {
                lock.writeLock().unlock();
            }
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
        IgniteUtils.shutdownAndAwaitTermination(notificationsThreadPool, 10, TimeUnit.SECONDS);

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
        SuperRoot rootNode = generator.createSuperRoot();

        fillFromPrefixMap(rootNode, toPrefixMap(latest));

        addDefaults(rootNode);

        ConfigValue conf = ConfigImpl.fromAnyRef(
                rootNode.accept(null, new ConverterToMapVisitor(false)), null
        );

        return renderConfig((ConfigObject) conf);
    }

    private static String renderConfig(ConfigObject conf) {
        Config newConfig = conf.toConfig();
        return newConfig.isEmpty()
                ? ""
                : newConfig.root().render(ConfigRenderOptions.concise());
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

    private <T> CompletableFuture<T> async(Supplier<T> supplier) {
        CompletableFuture<T> future = CompletableFuture.supplyAsync(supplier, workerThreadPool);
        futureTracker.registerFuture(future);

        return future;
    }

    private void sendNotificationAsync(Data data) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(
                () -> lsnrRef.get().onEntriesChanged(data),
                notificationsThreadPool
        );

        futureTracker.registerFuture(future);
    }
}
