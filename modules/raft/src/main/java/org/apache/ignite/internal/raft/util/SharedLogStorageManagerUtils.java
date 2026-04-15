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

package org.apache.ignite.internal.raft.util;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.raft.configuration.LogStorageConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.DefaultLogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.LogStorageException;
import org.apache.ignite.internal.raft.storage.impl.RocksDbLogStorageOptions;
import org.apache.ignite.internal.raft.storage.logit.LogitLogStorageManager;
import org.apache.ignite.internal.raft.storage.segstore.SegmentLogStorageManager;
import org.apache.ignite.internal.raft.storage.segstore.SegmentLogStorageOptions;
import org.apache.ignite.raft.jraft.storage.logit.option.StoreOptions;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/** Utility methods for creating {@link LogStorageManager}is for the Shared Log. */
public class SharedLogStorageManagerUtils {
    /**
     * Enables logit log storage. {@code false} by default. This is a temporary property, that should only be used for testing and comparing
     * the two storages.
     */
    public static final String LOGIT_STORAGE_ENABLED_PROPERTY = "LOGIT_STORAGE_ENABLED";

    private static final boolean LOGIT_STORAGE_ENABLED_PROPERTY_DEFAULT = false;

    /**
     * Creates a LogStorageManager with {@link DefaultLogStorageManager} or {@link LogitLogStorageManager} implementation depending on
     * LOGIT_STORAGE_ENABLED_PROPERTY and fsync set to true.
     */
    @TestOnly
    public static LogStorageManager create(String nodeName, Path logStoragePath, LogStorageConfiguration logStorageConfig) {
        var segmentLogStorageOptions = new SegmentLogStorageOptions(1, logStorageConfig, new FailureManager(new NoOpFailureHandler()));

        return create("test", nodeName, logStoragePath, true, RocksDbLogStorageOptions.defaults(), segmentLogStorageOptions);
    }

    /**
     * Creates a LogStorageManager with {@link SegmentLogStorageManager}, {@link DefaultLogStorageManager} or {@link LogitLogStorageManager}
     * implementation depending on SEGSTORE_ENABLED_PROPERTY / LOGIT_STORAGE_ENABLED_PROPERTY.
     */
    public static LogStorageManager create(
            String factoryName,
            String nodeName,
            Path logStoragePath,
            boolean fsync,
            RocksDbLogStorageOptions rocksDbStoreSpecificOptions,
            @Nullable SegmentLogStorageOptions segmentStoreSpecificOptions
    ) {
        if (!IgniteSystemProperties.segmentLogStorageEnabled()) {
            return IgniteSystemProperties.getBoolean(LOGIT_STORAGE_ENABLED_PROPERTY, LOGIT_STORAGE_ENABLED_PROPERTY_DEFAULT)
                    ? new LogitLogStorageManager(nodeName, new StoreOptions(), logStoragePath)
                    : new DefaultLogStorageManager(factoryName, nodeName, logStoragePath, fsync, rocksDbStoreSpecificOptions);
        }

        assert segmentStoreSpecificOptions != null;

        int stripes = segmentStoreSpecificOptions.stripes();
        LogStorageConfiguration storageConfiguration = segmentStoreSpecificOptions.configuration();
        FailureProcessor failureProcessor = segmentStoreSpecificOptions.failureProcessor();

        try {
            return new SegmentLogStorageManager(
                    nodeName,
                    factoryName,
                    logStoragePath,
                    stripes,
                    failureProcessor,
                    fsync,
                    storageConfiguration
            );
        } catch (IOException e) {
            throw new LogStorageException("Couldn't create SegmentLogStorageManager", e);
        }
    }
}
