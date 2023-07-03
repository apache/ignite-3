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
 * The listener which receives and handles the Meta Storage revision update. No listeners for revision {@code N+1} will be invoked until
 * all listeners for revision {@code N} are completed.
 * Also, this listener is only triggered strictly after all {@link WatchListener#onUpdate(WatchEvent)} are invoked (but the returned futures
 * might not still be completed completed), for the specified revision.
 */
@FunctionalInterface
public interface RevisionUpdateListener {
    /**
     * Callback that will be invoked when a Meta Storage revision update has been received.
     *
     * @param revision Meta Storage revision.
     * @return Future that will be completed when the event is processed.
     */
    CompletableFuture<?> onUpdated(long revision);
}
