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

package org.apache.ignite.internal.catalog.storage;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * Distributed log of catalog updates.
 */
public interface UpdateLog extends IgniteComponent {
    /**
     * Appends the given update to the log if the update with that version does not exist.
     *
     * <p>The version of {@link VersionedUpdate} must be greater by 1 of the version of previously
     * appended update, otherwise current update will be rejected. The versions of already appended updates should be tracked by registering
     * {@link OnUpdateHandler update handler} (see {@link #registerUpdateHandler(OnUpdateHandler)}).
     *
     * @param update An update to append to the log.
     * @return A {@code true} if update has been successfully appended, {@code false} otherwise if update with the same version already
     *         exists, and operation should be retried with new version.
     */
    CompletableFuture<Boolean> append(VersionedUpdate update);

    /**
     * Saves a snapshot entry and drop updates of previous versions from the log, if supported, otherwise do nothing.
     *
     * @param snapshotEntry An entry, which represents a result of merging updates of previous versions.
     * @return A {@code true} if snapshot has been successfully written, {@code false} otherwise if a snapshot with the same or greater
     *         version already exists.
     */
    CompletableFuture<Boolean> saveSnapshot(SnapshotEntry snapshotEntry);

    /**
     * Registers a handler to keep track of appended updates.
     *
     * @param handler A handler to notify when new update was appended.
     */
    void registerUpdateHandler(OnUpdateHandler handler);

    /**
     * Starts the component.
     *
     * <p>Log replay is a part of a component start up process, thus the handler must
     * be registered prior to start is invoked, otherwise exception will be thrown.
     *
     * @param componentContext The component lifecycle context.
     * @return Future that will be completed when the asynchronous part of the start is processed.
     * @throws IgniteInternalException If no handler has been registered.
     */
    @Override
    CompletableFuture<Void> startAsync(ComponentContext componentContext) throws IgniteInternalException;

    /** An interface describing a handler that will receive notification when a new update is added to the log. */
    @FunctionalInterface
    interface OnUpdateHandler {
        /**
         * An actual handler that will be invoked when new update is appended to the log.
         *
         * @param update A new update.
         * @param metaStorageUpdateTimestamp Timestamp assigned to the update by the Metastorage.
         * @param causalityToken Causality token.
         * @return Handler future.
         */
        CompletableFuture<Void> handle(UpdateLogEvent update, HybridTimestamp metaStorageUpdateTimestamp, long causalityToken);
    }
}
