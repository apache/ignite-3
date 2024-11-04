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

import java.util.function.Consumer;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedView;
import org.apache.ignite.internal.configuration.SystemPropertyView;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/** Configuration for zones high availability configurations. */
public class DistributionZonesHighAvailabilityConfiguration {
    /**
     * Internal property that determines partition group members reset timeout after the partition group majority loss.
     *
     * <p>Default value is {@link #PARTITION_DISTRIBUTION_RESET_TIMEOUT_DEFAULT_VALUE}.</p>
     */
    static final String PARTITION_DISTRIBUTION_RESET_TIMEOUT = "partitionDistributionResetTimeout";

    /** Default value for the {@link #PARTITION_DISTRIBUTION_RESET_TIMEOUT}. */
    private static final int PARTITION_DISTRIBUTION_RESET_TIMEOUT_DEFAULT_VALUE = 0;

    private final SystemDistributedConfiguration systemDistributedConfig;

    /** Determines partition group reset timeout after a partition group majority loss. */
    private volatile int partitionDistributionResetTimeout;

    private final Consumer<Integer> partitionDistributionResetListener;

    /** Constructor. */
    public DistributionZonesHighAvailabilityConfiguration(
            SystemDistributedConfiguration systemDistributedConfig,
            Consumer<Integer> partitionDistributionResetListener) {
        this.systemDistributedConfig = systemDistributedConfig;
        this.partitionDistributionResetListener = partitionDistributionResetListener;
    }

    /** Starts component. */
    public void start() {
        systemDistributedConfig.listen(ctx -> {
            updateSystemProperties(ctx.newValue());

            return nullCompletedFuture();
        });
    }

    /** Starts the component and initializes the configuration immediately. */
    @TestOnly
    void startAndInit() {
        start();

        updateSystemProperties(systemDistributedConfig.value());
    }

    /** Returns partition group reset timeout after a partition group majority loss. */
    public int partitionDistributionResetTimeout() {
        return partitionDistributionResetTimeout;
    }

    private void updateSystemProperties(SystemDistributedView view) {
        partitionDistributionResetTimeout = intValue(
                view,
                PARTITION_DISTRIBUTION_RESET_TIMEOUT,
                PARTITION_DISTRIBUTION_RESET_TIMEOUT_DEFAULT_VALUE
        );
        partitionDistributionResetListener.accept(partitionDistributionResetTimeout);
    }

    private static int intValue(SystemDistributedView systemDistributedView, String systemPropertyName, int defaultValue) {
        return intValue(systemDistributedView.properties().get(systemPropertyName), defaultValue);
    }

    private static int intValue(@Nullable SystemPropertyView systemPropertyView, int defaultValue) {
        if (systemPropertyView == null) {
            return defaultValue;
        }

        // There should be no errors, the {@code NonNegativeLongNumberSystemPropertyValueValidator} should work.
        return Integer.parseInt(systemPropertyView.propertyValue());
    }
}
