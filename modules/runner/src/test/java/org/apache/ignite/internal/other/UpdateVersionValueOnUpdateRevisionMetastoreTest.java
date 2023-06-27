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

package org.apache.ignite.internal.other;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl.withDefaultValidators;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.ByteArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class checks that updating the versioned value will only happen when the revision is updated by the metastore and not by the
 * configuration.
 */
// TODO: IGNITE-19801 подумать на счет теста скорее наверное нужен другой
public class UpdateVersionValueOnUpdateRevisionMetastoreTest {
    @ConfigurationRoot(rootName = "vv_test", type = DISTRIBUTED)
    public static class VvConfigurationSchema {
        @Value(hasDefault = true)
        public int val = 10;
    }

    private VaultManager vaultManager;

    private MetaStorageManager metaStorageManager;

    private ConfigurationStorage configStorage;

    private ConfigurationManager configManager;

    @BeforeEach
    void setUp() {
        RootKey<VvConfiguration, VvView> rootKey = VvConfiguration.KEY;

        ConfigurationTreeGenerator configTreeGenerator = new ConfigurationTreeGenerator(rootKey);

        vaultManager = new VaultManager(new InMemoryVaultService());

        metaStorageManager = StandaloneMetaStorageManager.create(vaultManager);

        configStorage = new DistributedConfigurationStorage(metaStorageManager, vaultManager);

        configManager = new ConfigurationManager(
                List.of(rootKey),
                configStorage,
                configTreeGenerator,
                withDefaultValidators(configTreeGenerator, Set.of())
        );

        vaultManager.start();
        metaStorageManager.start();
        configManager.start();

        assertThat(metaStorageManager.deployWatches(), willSucceedFast());
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                vaultManager == null ? null : vaultManager::stop,
                metaStorageManager == null ? null : metaStorageManager::stop,
                configStorage == null ? null : configStorage::close,
                configManager == null ? null : configManager::stop
        );
    }

    @Test
    void test() {
        IncrementalVersionedValue<Integer> vv = new IncrementalVersionedValue<>(createObservableRevisionUpdater(), () -> 10);

        VvConfiguration config = configManager.configurationRegistry().getConfiguration(VvConfiguration.KEY);

        config.val().listen(ctx -> vv.update(ctx.storageRevision(), UpdateVersionValueOnUpdateRevisionMetastoreTest::updateVv));

        assertThat(config.val().update(10), willSucceedFast());

        // Check that updating the metastore does not prematurely complete the VersionedValue by configuration.
        CompletableFuture<Void> updateVvInsideWatchListenerFuture = new CompletableFuture<>();

        String prefix = "vv_test";

        metaStorageManager.registerPrefixWatch(ByteArray.fromString(prefix), createWatchListener(vv, updateVvInsideWatchListenerFuture));

        assertThat(metaStorageManager.put(ByteArray.fromString(prefix + "_0"), "".getBytes(UTF_8)), willSucceedFast());

        assertThat(updateVvInsideWatchListenerFuture, willSucceedFast());
    }

    private Consumer<LongFunction<CompletableFuture<?>>> createObservableRevisionUpdater() {
        return c -> metaStorageManager.registerUpdateRevisionListener(revision -> (CompletableFuture<Void>) c.apply(revision));
    }

    private static WatchListener createWatchListener(
            IncrementalVersionedValue<Integer> vv,
            CompletableFuture<Void> updateVvInsideWatchListenerFuture
    ) {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                try {
                    return vv.update(event.revision(), UpdateVersionValueOnUpdateRevisionMetastoreTest::updateVv)
                            .thenAccept(integer -> {/* no-op */})
                            .whenComplete((integer, throwable) -> {
                                if (throwable == null) {
                                    updateVvInsideWatchListenerFuture.complete(null);
                                } else {
                                    updateVvInsideWatchListenerFuture.completeExceptionally(throwable);
                                }
                            });
                } catch (Throwable t) {
                    updateVvInsideWatchListenerFuture.completeExceptionally(t);

                    return failedFuture(t);
                }
            }

            @Override
            public void onError(Throwable e) {
                updateVvInsideWatchListenerFuture.completeExceptionally(e);
            }
        };
    }

    private static CompletableFuture<Integer> updateVv(Integer i, Throwable throwable) {
        return throwable != null ? failedFuture(throwable) : completedFuture(i + 1);
    }
}
