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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.Entry;

/**
 * Listener that gets invoked when {@link WatchProcessor}'s internal notification chain future is updated.
 */
@FunctionalInterface
public interface NotificationEnqueuedListener {
    /**
     * Notifies this listener that {@link WatchProcessor}'s internal notification chain future is updated.
     *
     * <p>This will always be run under the same lock under which the notification future is updated.
     *
     * <p>This must not do any I/O or block for a long time.
     *
     * @param newNotificationFuture New notification future.
     * @param entries Entries corresponding to the update (empty if the notification is not about a new revision,
     *     but about Metastorage safe time advancement.
     * @param timestamp Metastorage timestamp.
     */
    void onEnqueued(CompletableFuture<Void> newNotificationFuture, List<Entry> entries, HybridTimestamp timestamp);
}
