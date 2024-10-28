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

package org.apache.ignite.internal.distributionzones.configuration;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedView;
import org.apache.ignite.internal.configuration.SystemPropertyView;
import org.jetbrains.annotations.Nullable;

/** Configuration for zones high availability configurations. */
public class DistributionZonesHighAvailabilityConfiguration {
    /**
     * Internal property that determines scale down timeout after partition group majority loss.
     *
     * <p>Default value is {@link #RESET_SCALE_DOWN_DEFAULT_VALUE}.</p>
     */
    public static final String PARTITION_DISTRIBUTION_RESET_SCALE_DOWN = "partitionDistributionResetScaleDown";

    /** Default value for the {@link #PARTITION_DISTRIBUTION_RESET_SCALE_DOWN}. */
    public static final long RESET_SCALE_DOWN_DEFAULT_VALUE = 0;

    private final SystemDistributedConfiguration systemDistributedConfig;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /** Guarded by {@link #rwLock}. */
    private long partitionDistributionResetScaleDown;

    /** Constructor. */
    DistributionZonesHighAvailabilityConfiguration(SystemDistributedConfiguration systemDistributedConfig) {
        this.systemDistributedConfig = systemDistributedConfig;
    }

    /** Starts component. */
    void start() {
        systemDistributedConfig.listen(ctx -> {
            updateSystemProperties(ctx.newValue());

            return nullCompletedFuture();
        });
    }

    /** Returns compaction start interval (in milliseconds). */
    long partitionDistributionResetScaleDown() {
        rwLock.readLock().lock();

        try {
            return partitionDistributionResetScaleDown;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void updateSystemProperties(SystemDistributedView view) {
        rwLock.writeLock().lock();

        try {
            partitionDistributionResetScaleDown = longValue(view, PARTITION_DISTRIBUTION_RESET_SCALE_DOWN, RESET_SCALE_DOWN_DEFAULT_VALUE);
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

        // There should be no errors, the {@code NonNegativeLongNumberSystemPropertyValueValidator} should work.
        return Long.parseLong(systemPropertyView.propertyValue());
    }
}
