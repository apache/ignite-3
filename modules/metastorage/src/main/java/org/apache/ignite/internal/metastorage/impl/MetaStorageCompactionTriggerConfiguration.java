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

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedView;
import org.apache.ignite.internal.configuration.SystemPropertyView;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.Nullable;

/** Configuration for {@link MetaStorageCompactionTrigger}. */
class MetaStorageCompactionTriggerConfiguration {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageCompactionTriggerConfiguration.class);

    /** Compaction start interval (in milliseconds). Default value is {@link #INTERVAL_DEFAULT_VALUE}. */
    private static final String INTERVAL_SYSTEM_PROPERTY_NAME = "metastorageCompactionInterval";

    /** Data availability time (in milliseconds). Default value is {@link #DATA_AVAILABILITY_TIME_DEFAULT_VALUE}. */
    private static final String DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME = "metastorageCompactionDataAvailabilityTime";

    // TODO: IGNITE-23280 Make default 1 minute
    private static final long INTERVAL_DEFAULT_VALUE = Long.MAX_VALUE;

    // TODO: IGNITE-23280 Make default 1 hour
    private static final long DATA_AVAILABILITY_TIME_DEFAULT_VALUE = Long.MAX_VALUE;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /** Guarded by {@link #rwLock}. */
    private long interval;

    /** Guarded by {@link #rwLock}. */
    private long dataAvailabilityTime;

    MetaStorageCompactionTriggerConfiguration(SystemDistributedConfiguration systemDistributedConfiguration) {
        updateSystemProperties(systemDistributedConfiguration.value());

        systemDistributedConfiguration.listen(ctx -> {
            updateSystemProperties(ctx.newValue());

            return nullCompletedFuture();
        });
    }

    /** Returns compaction start interval (in milliseconds). */
    long interval() {
        rwLock.readLock().lock();

        try {
            return interval;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /** Data availability time (in milliseconds). */
    long dataAvailabilityTime() {
        rwLock.readLock().lock();

        try {
            return dataAvailabilityTime;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void updateSystemProperties(SystemDistributedView view) {
        rwLock.writeLock().lock();

        try {
            interval = longValue(view, INTERVAL_SYSTEM_PROPERTY_NAME, INTERVAL_DEFAULT_VALUE);
            dataAvailabilityTime = longValue(view, DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME, DATA_AVAILABILITY_TIME_DEFAULT_VALUE);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private static long longValue(SystemDistributedView systemDistributedView, String systemPropertyName, long defaultValue) {
        return longValue(systemDistributedView.properties().get(systemPropertyName), systemPropertyName, defaultValue);
    }

    private static long longValue(@Nullable SystemPropertyView systemPropertyView, String systemPropertyName, long defaultValue) {
        if (systemPropertyView == null) {
            return defaultValue;
        }

        try {
            return Long.parseLong(systemPropertyView.propertyValue());
        } catch (NumberFormatException e) {
            LOG.error(
                    "Invalid format for system property '{}' expecting a long value, default value '{}' will be used",
                    e,
                    systemPropertyName, defaultValue
            );

            return defaultValue;
        }
    }
}
