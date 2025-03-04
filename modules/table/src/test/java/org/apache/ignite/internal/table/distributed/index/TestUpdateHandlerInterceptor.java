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

package org.apache.ignite.internal.table.distributed.index;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogTestUtils.UpdateHandlerInterceptor;
import org.apache.ignite.internal.catalog.storage.UpdateLogEvent;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

class TestUpdateHandlerInterceptor extends UpdateHandlerInterceptor {
    private volatile @Nullable CompletableFuture<Void> dropEventsFuture;

    @Override
    public CompletableFuture<Void> handle(UpdateLogEvent update, HybridTimestamp metaStorageUpdateTimestamp, long causalityToken) {
        CompletableFuture<Void> future = dropEventsFuture;

        if (future != null) {
            future.complete(null);

            return nullCompletedFuture();
        }

        return delegate().handle(update, metaStorageUpdateTimestamp, causalityToken);
    }

    CompletableFuture<Void> startDropEvents() {
        var future = new CompletableFuture<Void>();

        dropEventsFuture = future;

        return future;
    }

    void stopDropEvents() {
        dropEventsFuture = null;
    }
}
