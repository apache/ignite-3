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

package org.apache.ignite.internal.catalog;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * The catalog manager provides schema manipulation methods and is responsible for managing distributed operations.
 */
public interface CatalogManager extends IgniteComponent, CatalogService {
    /**
     * Initial update timestamp for a catalog descriptor, this token is valid only before the first call of
     * {@link UpdateEntry#applyUpdate(Catalog, HybridTimestamp)}.
     *
     * <p>After that {@link CatalogObjectDescriptor#updateTimestamp()} will be initialised with a timestamp from
     * {@link UpdateEntry#applyUpdate(Catalog, HybridTimestamp)}
     */
    HybridTimestamp INITIAL_TIMESTAMP = HybridTimestamp.MIN_VALUE;

    /**
     * Executes given command.
     *
     * @param command Command to execute.
     * @return Future representing result of execution with the created catalog version.
     */
    CompletableFuture<CatalogApplyResult> execute(CatalogCommand command);

    /**
     * Executes given list of commands atomically. That is, either all commands will be applied at once
     * or neither of them. The whole bulk will increment catalog's version by a single point.
     *
     * @param commands Commands to execute.
     * @return Future representing result of execution with the created catalog version.
     */
    CompletableFuture<CatalogApplyResult> execute(List<CatalogCommand> commands);

    /**
     * Returns a future, which completes when empty catalog is initialised. Otherwise this future completes upon startup.
     */
    CompletableFuture<Void> catalogInitializationFuture();
}
