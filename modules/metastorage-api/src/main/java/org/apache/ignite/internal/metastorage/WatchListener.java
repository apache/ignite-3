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

package org.apache.ignite.internal.metastorage;

import java.util.concurrent.CompletableFuture;

/**
 * The listener which receives and handles watch updates.
 */
public interface WatchListener {
    /**
     * Returns a unique identifier for this Watch. This identifier should never change between node restarts and must uniquely identify
     * a Watch among all Watches on a local node.
     */
    String id();

    /**
     * The method will be called on each meta storage update.
     *
     * @param event A single event or a batch. The batch always contains updates for specific revision.
     * @return Future that will be completed when the event is processed.
     */
    CompletableFuture<Void> onUpdate(WatchEvent event);

    /**
     * Callback that will be invoked if a Meta Storage update has been received, but the modified entries do not match the given Watch.
     *
     * @param revision Meta Storage revision.
     * @return Future that will be completed when the event is processed.
     */
    default CompletableFuture<Void> onRevisionUpdated(long revision) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * The method will be called in case of an error occurred. The listener and corresponding watch will be unregistered.
     *
     * @param e Exception.
     */
    void onError(Throwable e);
}
