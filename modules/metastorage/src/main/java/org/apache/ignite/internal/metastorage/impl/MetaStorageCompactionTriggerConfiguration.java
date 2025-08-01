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

package org.apache.ignite.internal.metastorage.impl;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedView;
import org.apache.ignite.internal.configuration.SystemPropertyView;
import org.jetbrains.annotations.Nullable;

/** Configuration for metastorage compaction triggering based on distributed system properties. */
public class MetaStorageCompactionTriggerConfiguration {
    /**
     * Internal property that determines the interval between compaction initiations (in milliseconds).
     *
     * <p>Default value is {@link #INTERVAL_DEFAULT_VALUE}.</p>
     */
    public static final String INTERVAL_SYSTEM_PROPERTY_NAME = "metastorageCompactionInterval";

    /**
     * Internal property that defines compaction data availability time (in milliseconds).
     *
     * <p>Default value is {@link #DATA_AVAILABILITY_TIME_DEFAULT_VALUE}.</p>
     */
    public static final String DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME = "metastorageCompactionDataAvailabilityTime";

    /** Default value for the {@link #INTERVAL_SYSTEM_PROPERTY_NAME} (in milliseconds). */
    public static final long INTERVAL_DEFAULT_VALUE = TimeUnit.MINUTES.toMillis(1);

    /** Default value for the {@link #DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME} (in milliseconds). */
    public static final long DATA_AVAILABILITY_TIME_DEFAULT_VALUE = TimeUnit.HOURS.toMillis(1);

    private final SystemDistributedConfiguration systemDistributedConfig;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /** Guarded by {@link #rwLock}. */
    private long interval;

    /** Guarded by {@link #rwLock}. */
    private long dataAvailabilityTime;

    /** Guarded by {@link #rwLock}. */
    private boolean inited;

    /** Constructor. */
    MetaStorageCompactionTriggerConfiguration(SystemDistributedConfiguration systemDistributedConfig) {
        this.systemDistributedConfig = systemDistributedConfig;

        systemDistributedConfig.listen(ctx -> {
            updateSystemProperties(ctx.newValue());

            return nullCompletedFuture();
        });
    }

    /** Initializes the configuration from the distributed system configuration at component startup. */
    void init() {
        updateSystemProperties(systemDistributedConfig.value());
    }

    /** Returns compaction start interval (in milliseconds). */
    long interval() {
        rwLock.readLock().lock();

        try {
            assert inited : "Configuration has not yet been initialized from the distributed system configuration";

            return interval;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /** Data availability time (in milliseconds). */
    long dataAvailabilityTime() {
        rwLock.readLock().lock();

        try {
            assert inited : "Configuration has not yet been initialized from the distributed system configuration";

            return dataAvailabilityTime;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void updateSystemProperties(SystemDistributedView view) {
        rwLock.writeLock().lock();

        try {
            inited = true;

            interval = longValue(view, INTERVAL_SYSTEM_PROPERTY_NAME, INTERVAL_DEFAULT_VALUE);
            dataAvailabilityTime = longValue(view, DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME, DATA_AVAILABILITY_TIME_DEFAULT_VALUE);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private static long longValue(SystemDistributedView systemDistributedView, String systemPropertyName, long defaultValue) {
        return longValue(systemDistributedView.properties().get(systemPropertyName), defaultValue);
    }

    private static long longValue(@Nullable SystemPropertyView systemPropertyView, long defaultValue) {
        if (systemPropertyView == null) {
            return defaultValue;
        }

        // There should be no errors, the validator should work.
        return Long.parseLong(systemPropertyView.propertyValue());
    }
}
