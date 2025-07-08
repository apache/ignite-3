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
 * Allows to work with schema safe time. This safe time stems from Metastorage safe time (as schemas are delivered using Metastorage),
 * but it might grow faster as it only concerns schema-related (currently, it means Catalog-related) Metastorage updates.
 *
 * <p>If latest schema update happened in revision N and notifications of all watch listeners for that revision
 * (including Catalog listeners) have been completed, then a subsequent Metastorage event (that is, a revision event for some revision M
 * where M > N, or an idle safe time event with timestamp above the timestamp of revision N)
 * <ul>
 * <li>which does not relate to schemas (i.e. it's not a Catalog update) will advance schema safe time immediately (without waiting
 * for completion of futures of its watch listeners);</li>
 * <li>which relates to schemas (i.e. it's a Catalog update) will advance schema safe time only after the completion of futures
 * of its watch listeners).
 * </ul>
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface SchemaSafeTimeTracker {
    /**
     * Waits for schema safe time to reach the provided timestamp. That is, when the returned future completes, it is guaranteed
     * that all schema updates that should be visible at the provided timestamp are applied and the corresponding listeners finished
     * execution (so schemas are actual up to the provided timestamp).
     *
     * @param hybridTimestamp Timestamp in question.
     */
    CompletableFuture<Void> waitFor(HybridTimestamp hybridTimestamp);
}
