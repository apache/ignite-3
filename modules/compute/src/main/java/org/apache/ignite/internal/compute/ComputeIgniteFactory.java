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

package org.apache.ignite.internal.compute;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;

/**
 * Factory for creating per-job scoped {@link Ignite} instances.
 *
 * <p>Each compute job carries an observable timestamp from its initiator client.
 * Instead of updating a global tracker (which would cause "timestamp pollution"
 * across unrelated jobs), a per-job {@link HybridTimestampTracker} is created
 * and used to scope the {@link Ignite} instance given to the job.
 */
@FunctionalInterface
public interface ComputeIgniteFactory {
    /**
     * Creates an {@link Ignite} instance scoped to a specific compute job.
     *
     * @param tracker Per-job hybrid timestamp tracker.
     * @return An Ignite instance that uses the given tracker for transactions and SQL.
     */
    Ignite createForJob(HybridTimestampTracker tracker);
}
