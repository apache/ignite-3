/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.recovery;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.notifications.ConfigurationStorageRevisionListener;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.manager.EventParameters;
import org.apache.ignite.internal.metastorage.event.MetaStorageEvent;
import org.apache.ignite.internal.metastorage.event.MetaStorageEventParameters;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.IgniteSystemProperties;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Configuration listener class that is intended to complete catch-up future during recovery when configuration
 * is up-to-date.
 */
public class ConfigurationCatchUpListener implements EventListener<MetaStorageEventParameters> {
    /** Configuration catch-up difference property name. */
    public static final String CONFIGURATION_CATCH_UP_DIFFERENCE_PROPERTY = "CONFIGURATION_CATCH_UP_DIFFERENCE";

    /**
     * Difference between the local node applied revision and distributed data storage revision on start.
     * TODO: IGNITE-16488 Make this property adjustable and remove system property.
     */
    private final int configurationCatchUpDifference =
            IgniteSystemProperties.getInteger(CONFIGURATION_CATCH_UP_DIFFERENCE_PROPERTY, 100);

    /** Revision to catch up. */
    private volatile long targetRevision = -1;

    /** Catch-up future. */
    private final CompletableFuture<?> catchUpFuture;

    /** Configuration storage. */
    private final ConfigurationStorage cfgStorage;

    /** Mutex for updating target revision. */
    private final Object targetRevisionUpdateMutex = new Object();

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param catchUpFuture Catch-up future.
     */
    public ConfigurationCatchUpListener(ConfigurationStorage cfgStorage, CompletableFuture<?> catchUpFuture, IgniteLogger log) {
        this.cfgStorage = cfgStorage;
        this.catchUpFuture = catchUpFuture;
        this.log = log;
    }

    /**
     * Checks the node up to date by distributed configuration.
     *
     * @param targetRevision Configuration revision.
     * @param appliedRevision Last applied node revision.
     * @return True when the applied revision is great enough for node recovery to complete, false otherwise.
     */
    private boolean isConfigurationUpToDate(long targetRevision, long appliedRevision) {
        return targetRevision - configurationCatchUpDifference <= appliedRevision;
    }

    /**
     * Retrieve distribute configuration revision and check whether local revision is great enough to complete the recovery.
     *
     * @param appliedRevision Applied revision.
     */
    private CompletableFuture<Boolean> checkRevisionUpToDate(long appliedRevision) {
        return cfgStorage.lastRevision().thenApply(rev -> {
            synchronized (targetRevisionUpdateMutex) {
                assert rev >= appliedRevision : IgniteStringFormatter.format(
                    "Configuration revision must be greater than local node applied revision [msRev={}, appliedRev={}",
                    rev, appliedRevision);

                targetRevision = rev;

                log.info("Checking revision on recovery ["
                        + "targetRevision=" + targetRevision
                        + ", appliedRevision=" + appliedRevision
                        + ", acceptableDifference=" + configurationCatchUpDifference + ']'
                );

                if (isConfigurationUpToDate(targetRevision, appliedRevision)) {
                    catchUpFuture.complete(null);

                    return true;
                } else {
                    return false;
                }
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> notify(@NotNull MetaStorageEventParameters parameters, @Nullable Throwable exception) {
        long targetRev = targetRevision;
        long appliedRevision = parameters.causalityToken();

        if (targetRev >= 0) {
            return isConfigurationUpToDate(targetRev, appliedRevision) ? checkRevisionUpToDate(appliedRevision) : completedFuture(false);
        } else {
            return checkRevisionUpToDate(appliedRevision);
        }
    }
}
