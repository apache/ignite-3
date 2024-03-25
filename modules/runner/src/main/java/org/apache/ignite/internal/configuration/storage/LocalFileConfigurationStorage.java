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
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.fillFromPrefixMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.toPrefixMap;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException.Parse;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigSyntax;
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
import org.apache.ignite.configuration.ConfigurationDynamicDefaultsPatcher;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.configuration.ConfigurationDynamicDefaultsPatcherImpl;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.NodeConfigCreateException;
import org.apache.ignite.internal.configuration.NodeConfigParseException;
import org.apache.ignite.internal.configuration.NodeConfigWriteException;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.tree.ConverterToMapVisitor;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

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

    /** Configuration module, which provide configuration patches. **/
    private final ConfigurationModule module;

    /** Configuration changes listener. */
    private final AtomicReference<ConfigurationStorageListener> lsnrRef = new AtomicReference<>();

    /** Thread pool for configuration updates notifications. */
    private final ExecutorService notificationsThreadPool;

    /** Tracks all running futures. */
    private final InFlightFutures futureTracker = new InFlightFutures();

    /** Last revision for configuration. */
    private long lastRevision = 0L;

    /**
     * Constructor.
     *
     * @param configPath Path to node bootstrap configuration file.
     * @param generator Configuration tree generator.
     * @param module Configuration module, which provides configuration patches.
     */
    @TestOnly
    public LocalFileConfigurationStorage(Path configPath, ConfigurationTreeGenerator generator, @Nullable ConfigurationModule module) {
        this("test", configPath, generator, module);
    }

    /**
     * Constructor.
     *
     * @param configPath Path to node bootstrap configuration file.
     * @param generator Configuration tree generator.
     * @param module Configuration module, which provides configuration patches.
     */
    public LocalFileConfigurationStorage(
            String nodeName, Path configPath, ConfigurationTreeGenerator generator, @Nullable ConfigurationModule module) {
        this.configPath = configPath;
        this.generator = generator;
        this.tempConfigPath = configPath.resolveSibling(configPath.getFileName() + ".tmp");
        this.module = module;

        notificationsThreadPool = Executors.newFixedThreadPool(
                2, NamedThreadFactory.create(nodeName, "cfg-file", LOG)
        );

        checkAndRestoreConfigFile();
    }

    /**
     * Patch the local configs with defaults from provided {@link ConfigurationModule}.
     *
     * @param hocon Config string in Hocon format.
     * @param module Configuration module, which provides configuration patches.
     * @return Patched config string in Hocon format.
     */
    private String patch(String hocon, ConfigurationModule module) {
        if (module == null) {
            return hocon;
        }

        ConfigurationDynamicDefaultsPatcher localCfgDynamicDefaultsPatcher = new ConfigurationDynamicDefaultsPatcherImpl(
                module,
                generator
        );

        return localCfgDynamicDefaultsPatcher.patchWithDynamicDefaults(hocon);
    }

    @Override
    public CompletableFuture<Data> readDataOnRecovery() {
        lock.writeLock().lock();
        try {
            SuperRoot superRoot = generator.createSuperRoot();
            SuperRoot copiedSuperRoot = superRoot.copy();

            Config hocon = readHoconFromFile();
            HoconConverter.hoconSource(hocon.root()).descend(copiedSuperRoot);

            Map<String, Serializable> flattenedUpdatesMap = createFlattenedUpdatesMap(superRoot, copiedSuperRoot);
            flattenedUpdatesMap.forEach((key, value) -> {
                if (value != null) { // Filter defaults.
                    latest.put(key, value);
                }
            });

            return CompletableFuture.completedFuture(new Data(latest, lastRevision));
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Config readHoconFromFile() {
        checkAndRestoreConfigFile();

        try {
            String confString = Files.readString(configPath.toAbsolutePath());

            ConfigParseOptions parseOptions = ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF).setAllowMissing(false);

            return ConfigFactory.parseString(patch(confString, module), parseOptions);
        } catch (Parse | ConfigurationValidationException | IOException e) {
            throw new NodeConfigParseException("Failed to parse config content from file " + configPath, e);
        }
    }

    @Override
    public CompletableFuture<Map<String, ? extends Serializable>> readAllLatest(String prefix) {
        lock.readLock().lock();
        try {
            return CompletableFuture.completedFuture(
                    latest.entrySet()
                            .stream()
                            .filter(entry -> entry.getKey().startsWith(prefix))
                            .collect(toMap(Entry::getKey, Entry::getValue))
            );
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Serializable> readLatest(String key) {
        lock.readLock().lock();
        try {
            return CompletableFuture.completedFuture(latest.get(key));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Boolean> write(Map<String, ? extends Serializable> newValues, long ver) {
        lock.writeLock().lock();
        try {
            if (ver != lastRevision) {
                return falseCompletedFuture();
            }

            mergeAndSave(newValues);

            sendNotificationAsync(new Data(newValues, lastRevision));

            return trueCompletedFuture();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void mergeAndSave(Map<String, ? extends Serializable> newValues) {
        updateLatestState(newValues);
        saveConfigFile();
        lastRevision++;
    }

    private void updateLatestState(Map<String, ? extends Serializable> newValues) {
        newValues.forEach((key, value) -> {
            if (value == null) { // Null means that we should remove this entry.
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
    public CompletableFuture<Long> localRevision() {
        return lastRevision();
    }

    @Override
    public void close() {
        futureTracker.cancelInFlightFutures();

        IgniteUtils.shutdownAndAwaitTermination(notificationsThreadPool, 10, TimeUnit.SECONDS);
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

        Object transformed = rootNode.accept(
                null,
                null,
                ConverterToMapVisitor.builder()
                        .includeInternal(false)
                        .skipEmptyValues(true)
                        .maskSecretValues(false)
                        .build()
        );

        ConfigValue conf = ConfigImpl.fromAnyRef(transformed, null);

        return renderConfig((ConfigObject) conf);
    }

    private static String renderConfig(ConfigObject conf) {
        Config newConfig = conf.toConfig().resolve();
        return newConfig.isEmpty()
                ? ""
                : newConfig.root().render(ConfigRenderOptions.concise().setFormatted(true).setJson(false));
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

    private void sendNotificationAsync(Data data) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(
                () -> lsnrRef.get().onEntriesChanged(data),
                notificationsThreadPool
        );

        futureTracker.registerFuture(future);
    }
}
