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

package org.apache.ignite.internal.partition.replicator.schema;

import static java.util.function.Function.identity;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.SchemaSyncService;

/**
 * Decorates a {@link SchemaSyncService} to make sure that completion stages depending on the returned futures are always completed
 * either using the provided {@link Executor} or in the thread that executed the addition of the corresponding stage to the returned future.
 */
public class ExecutorInclinedSchemaSyncService implements SchemaSyncService {
    private final SchemaSyncService schemaSyncService;

    private final Executor completionExecutor;

    public ExecutorInclinedSchemaSyncService(SchemaSyncService schemaSyncService, Executor completionExecutor) {
        this.schemaSyncService = schemaSyncService;
        this.completionExecutor = completionExecutor;
    }

    @Override
    public CompletableFuture<Void> waitForMetadataCompleteness(HybridTimestamp ts) {
        CompletableFuture<Void> future = schemaSyncService.waitForMetadataCompleteness(ts);

        if (future.isDone()) {
            return future;
        }

        return future.thenApplyAsync(identity(), completionExecutor);
    }

}
