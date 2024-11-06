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
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.server.time.ClusterTime;
import org.apache.ignite.internal.util.CompletableFutures;

/**
 * {@link ClusterTime} implementation that thinks that it lives in the infinite future and, hence, it doesn't need to be ever waited for.
 */
public class InfiniteFutureClusterTime implements ClusterTime {
    @Override
    public HybridTimestamp now() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long nowLong() {
        throw new UnsupportedOperationException();
    }

    @Override
    public HybridTimestamp currentSafeTime() {
        return HybridTimestamp.MAX_VALUE;
    }

    @Override
    public CompletableFuture<Void> waitFor(HybridTimestamp time) {
        return CompletableFutures.nullCompletedFuture();
    }
}
