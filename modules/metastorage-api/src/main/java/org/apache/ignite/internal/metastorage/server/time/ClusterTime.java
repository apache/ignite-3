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

package org.apache.ignite.internal.metastorage.server.time;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;

/**
 * Cluster time with a hybrid clock instance and access to safe time.
 */
public interface ClusterTime {
    /**
     * Returns current cluster time.
     */
    HybridTimestamp now();

    /**
     * Returns current cluster time.
     */
    long nowLong();

    /**
     * Returns current safe time.
     */
    HybridTimestamp currentSafeTime();

    /**
     * Provides the future that is completed when cluster time reaches given one. If the time is greater or equal
     * then the given one, returns completed future.
     *
     * @param time Timestamp to wait for.
     * @return Future.
     */
    CompletableFuture<Void> waitFor(HybridTimestamp time);
}
