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

package org.apache.ignite.internal.metastorage.server;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.RevisionUpdateListener;

/**
 * Watch inhibitor that starts inhibiting on certain condition.
 */
public class ConditionalWatchInhibitor {
    private static final IgniteLogger LOG = Loggers.forClass(ConditionalWatchInhibitor.class);

    private final MetaStorageManager metaStorageManager;

    private CompletableFuture<Void> inhibitFuture;

    private RevisionUpdateListener listener;

    public ConditionalWatchInhibitor(MetaStorageManager metaStorageManager) {
        this.metaStorageManager = metaStorageManager;
    }

    /**
     * Starts inhibiting on condition.
     *
     * @param pred Condition.
     */
    public void startInhibit(Predicate<Long> pred) {
        inhibitFuture = new CompletableFuture<>();
        listener = rev -> {
            if (pred.test(rev)) {
                LOG.info("Started inhibiting, rev=" + rev);
                return inhibitFuture;
            } else {
                return nullCompletedFuture();
            }
        };

        metaStorageManager.registerRevisionUpdateListener(listener);
    }

    /**
     * Stop inhibiting.
     */
    public void stopInhibit() {
        inhibitFuture.complete(null);

        metaStorageManager.unregisterRevisionUpdateListener(listener);
    }
}
