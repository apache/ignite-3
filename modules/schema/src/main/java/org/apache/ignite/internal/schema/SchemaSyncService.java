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

package org.apache.ignite.internal.schema;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;

/**
 * Implements Schema Synchronization wait logic as defined in IEP-98.
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface SchemaSyncService {
    /**
     * Waits till metadata (like table/index schemas) is complete for the given timestamp. The 'complete' here means
     * that no metadata change can arrive later that would change how a table/index/etc looks at the given timestamp.
     *
     * <p>This also means that all listeners that react to all metadata changes up to the point of interest have completed
     * their execution.
     *
     * <p>This method does NOT provide any guarantees about whether listeners reacting to other types of events
     * (which are not schema changes) have completed their execution.
     *
     * @param ts Timestamp of interest.
     * @return Future that completes when it is safe to query the Catalog at the given timestamp (as its data will
     *     remain unchanged for the timestamp).
     */
    CompletableFuture<Void> waitForMetadataCompleteness(HybridTimestamp ts);
}
