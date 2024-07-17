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

package org.apache.ignite.internal.network.recovery;

/**
 * Allows to detect whether an ID identifying a node on the networking level is stale or not.
 * An ID becomes stale when a node having this ID disappears from the Physical Topology.
 */
@FunctionalInterface
public interface StaleIdDetector {
    /**
     * Returns {@code true} iff the given ID is stale.
     *
     * @param nodeId ID to check.
     * @return {@code true} iff the given ID is stale.
     */
    boolean isIdStale(String nodeId);
}
