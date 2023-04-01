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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
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
import java.util.stream.Collectors;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.configuration.NodeConfigCreateException;
import org.apache.ignite.internal.configuration.NodeConfigWriteException;
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

    /**
     * Path to config file.
     */
    private final Path configPath;

    /**
     * Path to temporary configuration storage.
     */
    private final Path tempConfigPath;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Latest state of last applied configuration.
     */
    private final Map<String, Serializable> latest = new ConcurrentHashMap<>();

    /**
     *  Configuration changes listener.
     *  */
    private final AtomicReference<ConfigurationStorageListener> lsnrRef = new AtomicReference<>();

    private final ExecutorService threadPool = Executors.newFixedThreadPool(2, new NamedThreadFactory("loc-cfg-file", LOG));

    private final InFlightFutures futureTracker = new InFlightFutures();

    private long lastRevision = 0L;

    /**
     * Constructor.
     *
     * @param configPath Path to node bootstrap configuration file.
     */
    public LocalFileConfigurationStorage(Path configPath) {
        this.configPath = configPath;
        tempConfigPath = configPath.resolveSibling(configPath.getFileName() + ".tmp");
        checkAndRestoreConfigFile();
    }

    @Override
    public CompletableFuture<Data> readDataOnRecovery() {
        return CompletableFuture.completedFuture(new Data(Collections.emptyMap(), 0));
    }

    @Override
    public CompletableFuture<Map<String, ? extends Serializable>> readAllLatest(String prefix) {
        lock.readLock().lock();
        try {
            checkAndRestoreConfigFile();
            Map<String, Serializable> map = latest.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().startsWith(prefix))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
            return CompletableFuture.completedFuture(map);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Serializable> readLatest(String key) {
        lock.readLock().lock();
        try {
            checkAndRestoreConfigFile();
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
                return CompletableFuture.completedFuture(false);
            }
            checkAndRestoreConfigFile();
            saveValues(newValues);
            latest.putAll(newValues);
            lastRevision++;
            runAsync(() -> lsnrRef.get().onEntriesChanged(new Data(newValues, lastRevision)));
            return CompletableFuture.completedFuture(true);
        } finally {
            lock.writeLock().unlock();
        }
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
        IgniteUtils.shutdownAndAwaitTermination(threadPool, 10, TimeUnit.SECONDS);

        futureTracker.cancelInFlightFutures();
    }

    private void saveValues(Map<String, ? extends Serializable> values) {
        try {
            Files.write(tempConfigPath, renderHoconString(values).getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.SYNC, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tempConfigPath, configPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new NodeConfigWriteException(
                    "Failed to write values " + values + " to config file.", e);
        }
    }

    /**
     * Convert provided map to Hocon String representation.
     *
     * @param values Values of configuration.
     * @return Configuration file string representation in HOCON format.
     */
    private String renderHoconString(Map<String, ? extends Serializable> values) {
        Map<String, Object> map = values.entrySet().stream().collect(Collectors.toMap(Entry::getKey, stringEntry -> {
            Serializable value = stringEntry.getValue();
            if (value.getClass().isArray()) {
                return Arrays.asList((Object[]) value);
            }
            return value;
        }));
        Config other = ConfigFactory.parseMap(map);
        Config newConfig = other.withFallback(parseConfigOptions()).resolve();
        return newConfig.isEmpty()
                ? ""
                : newConfig.root().render(ConfigRenderOptions.concise().setFormatted(true).setJson(false));
    }

    private Config parseConfigOptions() {
        return ConfigFactory.parseFile(
                configPath.toFile(),
                ConfigParseOptions.defaults().setAllowMissing(false));
    }

    /** Check that configuration file still exists and restore it with latest applied state in case it was deleted. */
    private void checkAndRestoreConfigFile() {
        if (!configPath.toFile().exists()) {
            try {
                if (configPath.toFile().createNewFile()) {
                    if (!latest.isEmpty()) {
                        saveValues(latest);
                    }
                } else {
                    throw new NodeConfigCreateException("Failed to re-create config file");
                }
            } catch (IOException e) {
                throw new NodeConfigWriteException("Failed to restore config file.", e);
            }
        }
    }
}
