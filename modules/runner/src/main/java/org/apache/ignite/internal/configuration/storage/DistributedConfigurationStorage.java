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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.revision;
import static org.apache.ignite.lang.util.StringUtils.incrementLastChar;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.configuration.util.ConfigurationSerializationUtil;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.ConditionType;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ByteArray;

/**
 * Distributed configuration storage.
 */
public class DistributedConfigurationStorage implements ConfigurationStorage {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DistributedConfigurationStorage.class);

    /** Prefix added to configuration keys to distinguish them in the meta storage. Must end with a dot. */
    private static final String DISTRIBUTED_PREFIX = "dst-cfg.";

    /**
     * Key for CAS-ing configuration keys to meta storage.
     */
    private static final ByteArray MASTER_KEY = new ByteArray(DISTRIBUTED_PREFIX + "$master$key");

    /**
     * Prefix for all keys in the distributed storage. This key is expected to be the first key in lexicographical order of distributed
     * configuration keys.
     */
    private static final ByteArray DST_KEYS_START_RANGE = new ByteArray(DISTRIBUTED_PREFIX);

    /**
     * This key is expected to be the last key in lexicographical order of distributed configuration keys. It is possible because keys are
     * in lexicographical order in meta storage and adding {@code (char)('.' + 1)} to the end will produce all keys with prefix
     * {@link DistributedConfigurationStorage#DISTRIBUTED_PREFIX}
     */
    private static final ByteArray DST_KEYS_END_RANGE = new ByteArray(incrementLastChar(DISTRIBUTED_PREFIX));

    /** Meta storage manager. */
    private final MetaStorageManager metaStorageMgr;

    /** Configuration changes listener. */
    private volatile ConfigurationStorageListener lsnr;

    /**
     * Currently known change id. Either matches or will soon match the Meta Storage revision of the latest configuration update. It is
     * possible that {@code changeId} is already updated but notifications are not yet handled, thus revision is valid but not applied. This
     * is fine.
     *
     * <p>Given that {@link #MASTER_KEY} is updated on every configuration change, one could assume that {@code changeId} matches the
     * revision of {@link #MASTER_KEY}.
     *
     * <p>This is true for all cases except for node restart. We use latest values after restart, so MetaStorage's local revision is used
     * instead. This fact has very important side effect: it's no longer possible to use {@link ConditionType#REV_EQUAL} on
     * {@link #MASTER_KEY} in {@link DistributedConfigurationStorage#write(Map, long)}. {@link ConditionType#REV_LESS_OR_EQUAL} must be
     * used instead.
     *
     * @see #MASTER_KEY
     * @see #write(Map, long)
     */
    private volatile long changeId;

    private final ExecutorService threadPool = Executors.newFixedThreadPool(4, new NamedThreadFactory("dst-cfg", LOG));

    private final InFlightFutures futureTracker = new InFlightFutures();

    /**
     * Constructor.
     *
     * @param metaStorageMgr Meta storage manager.
     */
    public DistributedConfigurationStorage(MetaStorageManager metaStorageMgr) {
        this.metaStorageMgr = metaStorageMgr;
    }

    @Override
    public void close() {
        IgniteUtils.shutdownAndAwaitTermination(threadPool, 10, TimeUnit.SECONDS);

        futureTracker.cancelInFlightFutures();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Map<String, ? extends Serializable>> readAllLatest(String prefix) {
        var rangeStart = new ByteArray(DISTRIBUTED_PREFIX + prefix);

        var rangeEnd = new ByteArray(incrementLastChar(DISTRIBUTED_PREFIX + prefix));

        var resultFuture = new CompletableFuture<Map<String, ? extends Serializable>>();

        metaStorageMgr.range(rangeStart, rangeEnd).subscribe(new Subscriber<>() {
            private final Map<String, Serializable> data = new HashMap<>();

            @Override
            public void onSubscribe(Subscription subscription) {
                // Request unlimited demand.
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Entry item) {
                byte[] key = item.key();

                // Skip the master key.
                if (Arrays.equals(key, MASTER_KEY.bytes())) {
                    return;
                }

                byte[] value = item.value();

                // Meta Storage should not return nulls and tombstones.
                assert !item.tombstone();
                assert value != null;

                String dataKey = new String(key, UTF_8).substring(DISTRIBUTED_PREFIX.length());

                data.put(dataKey, ConfigurationSerializationUtil.fromBytes(value));
            }

            @Override
            public void onError(Throwable throwable) {
                resultFuture.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                resultFuture.complete(data);
            }
        });

        return registerFuture(resultFuture);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Serializable> readLatest(String key) {
        return metaStorageMgr.get(new ByteArray(DISTRIBUTED_PREFIX + key))
                .thenApply(entry -> {
                    byte[] value = entry.value();

                    return value == null ? null : ConfigurationSerializationUtil.fromBytes(value);
                })
                .exceptionally(e -> {
                    throw new StorageException("Exception while reading data from Meta Storage", e);
                });
    }

    @Override
    public CompletableFuture<Data> readDataOnRecovery() throws StorageException {
        CompletableFuture<Data> future = metaStorageMgr.recoveryFinishedFuture()
                .thenApplyAsync(this::readDataOnRecovery0, threadPool);

        return registerFuture(future);
    }

    private Data readDataOnRecovery0(long cfgRevision) {
        var data = new HashMap<String, Serializable>();

        byte[] masterKey = MASTER_KEY.bytes();
        boolean sawMasterKey = false;

        try (Cursor<Entry> cursor = metaStorageMgr.getLocally(DST_KEYS_START_RANGE, DST_KEYS_END_RANGE, cfgRevision)) {
            for (Entry entry : cursor) {
                if (entry.tombstone()) {
                    continue;
                }

                byte[] key = entry.key();
                byte[] value = entry.value();

                // MetaStorage iterator should not return nulls as values.
                assert value != null;

                if (!sawMasterKey && Arrays.equals(masterKey, key)) {
                    sawMasterKey = true;

                    continue;
                }

                int startIdx = DST_KEYS_START_RANGE.length();

                int keyLengthWithoutPrefix = key.length - startIdx;

                var dataKey = new String(key, startIdx, keyLengthWithoutPrefix, UTF_8);

                data.put(dataKey, ConfigurationSerializationUtil.fromBytes(value));
            }
        } catch (Exception e) {
            throw new StorageException("Exception reading data on recovery", e);
        }

        assert data.isEmpty() || cfgRevision > 0;

        changeId = cfgRevision;

        return new Data(data, cfgRevision);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> write(Map<String, ? extends Serializable> newValues, long curChangeId) {
        assert curChangeId <= changeId;
        assert lsnr != null : "Configuration listener must be initialized before write.";

        if (curChangeId < changeId) {
            // This means that curChangeId is less than version and other node has already updated configuration and
            // write should be retried. Actual version will be set when watch and corresponding configuration listener
            // updates configuration.
            return CompletableFuture.completedFuture(false);
        }

        Set<Operation> operations = new HashSet<>();

        for (Map.Entry<String, ? extends Serializable> entry : newValues.entrySet()) {
            ByteArray key = new ByteArray(DISTRIBUTED_PREFIX + entry.getKey());

            if (entry.getValue() != null) {
                operations.add(Operations.put(key, ConfigurationSerializationUtil.toBytes(entry.getValue())));
            } else {
                operations.add(Operations.remove(key));
            }
        }

        operations.add(Operations.put(MASTER_KEY, ByteUtils.longToBytes(curChangeId)));

        // Condition for a valid MetaStorage data update. Several possibilities here:
        //  - First update ever, MASTER_KEY property must be absent from MetaStorage.
        //  - Current node has already performed some updates or received them from MetaStorage watch listener. In this
        //    case "curChangeId" must match the MASTER_KEY revision exactly.
        //  - Current node has been restarted and received updates from MetaStorage watch listeners after that. Same as
        //    above, "curChangeId" must match the MASTER_KEY revision exactly.
        //  - Current node has been restarted and have not received any updates from MetaStorage watch listeners yet.
        //    In this case "curChangeId" matches MetaStorage's local revision, which may or may not match the MASTER_KEY revision. Two
        //    options here:
        //     - MASTER_KEY is missing in local MetaStorage copy. This means that current node have not performed nor
        //       observed any configuration changes. Valid condition is "MASTER_KEY does not exist".
        //     - MASTER_KEY is present in local MetaStorage copy. The MASTER_KEY revision is unknown but is less than or
        //       equal to MetaStorage's local revision. Obviously, there have been no updates from the future yet. It's also guaranteed
        //       that the next received configuration update will have the MASTER_KEY revision strictly greater than
        //       current MetaStorage's local revision. This allows to conclude that "MASTER_KEY revision <= curChangeId" is a valid
        //       condition for update.
        // Joining all of the above, it's concluded that the following condition must be used:
        Condition condition = curChangeId == 0L
                ? notExists(MASTER_KEY)
                : or(notExists(MASTER_KEY), revision(MASTER_KEY).le(curChangeId));

        return metaStorageMgr.invoke(condition, operations, Set.of(Operations.noop()));
    }

    @Override
    public void registerConfigurationListener(ConfigurationStorageListener lsnr) {
        assert this.lsnr == null;

        this.lsnr = lsnr;

        metaStorageMgr.registerPrefixWatch(DST_KEYS_START_RANGE, new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent events) {
                Map<String, Serializable> data = IgniteUtils.newHashMap(events.entryEvents().size() - 1);

                Entry masterKeyEntry = null;

                for (EntryEvent event : events.entryEvents()) {
                    Entry e = event.newEntry();

                    if (Arrays.equals(e.key(), MASTER_KEY.bytes())) {
                        masterKeyEntry = e;
                    } else {
                        String key = new String(e.key(), UTF_8).substring(DISTRIBUTED_PREFIX.length());

                        Serializable value = e.value() == null ? null : ConfigurationSerializationUtil.fromBytes(e.value());

                        data.put(key, value);
                    }
                }

                // Contract of meta storage ensures that all updates of one revision will come in one batch.
                // Also masterKey should be updated every time when we update cfg.
                // That means that masterKey update must be included in the batch.
                assert masterKeyEntry != null;

                long newChangeId = masterKeyEntry.revision();

                assert newChangeId > changeId;

                changeId = newChangeId;

                return lsnr.onEntriesChanged(new Data(data, newChangeId));
            }

            @Override
            public void onError(Throwable e) {
                LOG.warn("Meta storage listener issue", e);
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public ConfigurationType type() {
        return ConfigurationType.DISTRIBUTED;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Long> lastRevision() {
        return metaStorageMgr.get(MASTER_KEY).thenApply(Entry::revision);
    }

    private <T> CompletableFuture<T> registerFuture(CompletableFuture<T> future) {
        futureTracker.registerFuture(future);

        return future;
    }
}
