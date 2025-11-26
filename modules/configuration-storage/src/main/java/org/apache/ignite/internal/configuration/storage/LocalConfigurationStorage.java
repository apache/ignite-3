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

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.configuration.util.ConfigurationSerializationUtil.fromBytes;
import static org.apache.ignite.internal.configuration.util.ConfigurationSerializationUtil.toBytes;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.StringUtils.incrementLastChar;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;

/**
 * Local configuration storage.
 */
public class LocalConfigurationStorage implements ConfigurationStorage {
    /** Prefix that we add to configuration keys to distinguish them in the Vault. */
    private static final String LOC_PREFIX = "loc-cfg.";

    /** Key for the storage revision version. */
    private static final ByteArray VERSION_KEY = new ByteArray(LOC_PREFIX + "$version");

    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LocalConfigurationStorage.class);

    /** Vault manager. */
    private final VaultManager vaultMgr;

    /** Configuration changes listener.*/
    private final AtomicReference<ConfigurationStorageListener> lsnrRef = new AtomicReference<>();

    /** Start key in range for searching local configuration keys. */
    private static final ByteArray LOC_KEYS_START_RANGE = ByteArray.fromString(LOC_PREFIX);

    /** End key in range for searching local configuration keys. */
    private static final ByteArray LOC_KEYS_END_RANGE = ByteArray.fromString(incrementLastChar(LOC_PREFIX));

    private final ExecutorService threadPool;

    private final InFlightFutures futureTracker = new InFlightFutures();

    /**
     * Future used to serialize writes to the storage.
     *
     * <p>Every write must wait before the previous write operation, bound to this future, completes.
     *
     * <p>Multi-threaded access is guarded by {@code writeSerializationLock}.
     */
    private CompletableFuture<Void> writeSerializationFuture = nullCompletedFuture();

    /** Lock for updating the reference to the {@code writeSerializationFuture}. */
    private final Object writeSerializationLock = new Object();

    /**
     * Constructor.
     *
     * @param vaultMgr Vault manager.
     */
    public LocalConfigurationStorage(String nodeName, VaultManager vaultMgr) {
        this.vaultMgr = vaultMgr;
        this.threadPool = Executors.newFixedThreadPool(4, IgniteThreadFactory.create(nodeName, "loc-cfg", LOG));
    }

    @Override
    public void close() {
        IgniteUtils.shutdownAndAwaitTermination(threadPool, 10, TimeUnit.SECONDS);

        futureTracker.cancelInFlightFutures();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Map<String, ? extends Serializable>> readAllLatest(String prefix) {
        var rangeStart = new ByteArray(LOC_PREFIX + prefix);

        var rangeEnd = new ByteArray(incrementLastChar(LOC_PREFIX + prefix));

        return readAll(rangeStart, rangeEnd).thenApply(Data::values);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Serializable> readLatest(String key) {
        return registerFuture(supplyAsync(() -> {
            try {
                VaultEntry entry = vaultMgr.get(new ByteArray(LOC_PREFIX + key));

                return entry == null ? null : fromBytes(entry.value());
            } catch (Exception e) {
                throw new StorageException("Exception while reading vault entry", e);
            }
        }, threadPool));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Data> readDataOnRecovery() {
        return readAll(LOC_KEYS_START_RANGE, LOC_KEYS_END_RANGE);
    }

    /**
     * Retrieves all data, which keys lie in between {@code [rangeStart, rangeEnd)}.
     */
    private CompletableFuture<Data> readAll(ByteArray rangeStart, ByteArray rangeEnd) {
        return registerFuture(supplyAsync(() -> {
            var data = new HashMap<String, Serializable>();

            long version = 0;

            try (Cursor<VaultEntry> cursor = vaultMgr.range(rangeStart, rangeEnd)) {
                for (VaultEntry entry : cursor) {
                    ByteArray key = entry.key();

                    Serializable value = fromBytes(entry.value());

                    if (key.equals(VERSION_KEY)) {
                        version = (Long) value;
                    } else {
                        data.put(removePrefix(key), value);
                    }
                }
            } catch (Exception e) {
                throw new StorageException("Exception when closing a Vault cursor", e);
            }

            return new Data(data, version);
        }, threadPool));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> write(Map<String, ? extends Serializable> newValues, long sentVersion) {
        synchronized (writeSerializationLock) {
            CompletableFuture<Boolean> writeFuture = registerFuture(writeSerializationFuture
                    .thenCompose(v -> lastRevision())
                    .thenComposeAsync(version -> {
                        if (version != sentVersion) {
                            return falseCompletedFuture();
                        }

                        ConfigurationStorageListener lsnr = lsnrRef.get();

                        assert lsnr != null : "Configuration listener must be initialized before write.";

                        Map<ByteArray, byte[]> data = IgniteUtils.newHashMap(newValues.size() + 1);

                        for (Map.Entry<String, ? extends Serializable> e : newValues.entrySet()) {
                            ByteArray key = ByteArray.fromString(LOC_PREFIX + e.getKey());

                            data.put(key, e.getValue() == null ? null : toBytes(e.getValue()));
                        }

                        byte[] previousVersion = data.put(VERSION_KEY, toBytes(version + 1));

                        if (previousVersion != null) {
                            throw new IllegalStateException(String.format(
                                    "\"%s\" is a reserved key and must not be changed externally",
                                    removePrefix(VERSION_KEY)
                            ));
                        }

                        Data entries = new Data(newValues, version + 1);

                        vaultMgr.putAll(data);

                        return lsnr.onEntriesChanged(entries).thenApply(v -> true);
                    }, threadPool));

            // ignore any errors on the write future, because we are only interested in its completion
            writeSerializationFuture = writeFuture.handle((v, e) -> null);

            return writeFuture;
        }
    }

    private static String removePrefix(ByteArray key) {
        return key.toString().substring(LOC_PREFIX.length());
    }

    /** {@inheritDoc} */
    @Override
    public void registerConfigurationListener(ConfigurationStorageListener lsnr) {
        if (!lsnrRef.compareAndSet(null, lsnr)) {
            LOG.debug("Configuration listener has already been set");
        }
    }

    /** {@inheritDoc} */
    @Override
    public ConfigurationType type() {
        return ConfigurationType.LOCAL;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Long> lastRevision() {
        return registerFuture(supplyAsync(() -> {
            VaultEntry entry = vaultMgr.get(VERSION_KEY);

            return entry == null ? 0 : (Long) fromBytes(entry.value());
        }, threadPool));
    }

    @Override
    public CompletableFuture<Long> localRevision() {
        return lastRevision();
    }

    private <T> CompletableFuture<T> registerFuture(CompletableFuture<T> future) {
        futureTracker.registerFuture(future);

        return future;
    }
}
