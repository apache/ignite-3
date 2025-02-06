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

package org.apache.ignite.internal.sql.engine.exec.ddl;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.clusterWideEnsuredActivationTimestamp;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogApplyResult;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.AbstractCreateIndexCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.MakeIndexAvailableEventParameters;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/** DDL commands handler. */
public class DdlCommandHandler implements LifecycleAware {
    private final CatalogManager catalogManager;

    private final ClockService clockService;

    private final InFlightFutures inFlightFutures = new InFlightFutures();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /**
     * Constructor.
     */
    public DdlCommandHandler(
            CatalogManager catalogManager,
            ClockService clockService
    ) {
        this.catalogManager = catalogManager;
        this.clockService = clockService;
    }

    /**
     * Submits given list of commands at once.
     *
     * <p>The whole list is submitted atomically. The result of applying of any individual command in case of conditional statements
     * may be checked by calling {@link CatalogApplyResult#isApplied(int)} providing 0-based index of command in question. If exception
     * is thrown during processing of any command from batch, then none of the commands will be applied.
     *
     * @param batch A batch of command to execute.
     * @return Future containing result of applying a list of commands to catalog.
     */
    public CompletableFuture<CatalogApplyResult> handle(List<CatalogCommand> batch) {
        CompletableFuture<CatalogApplyResult> fut = catalogManager.execute(batch);

        boolean hasCreateIndexCommand = batch.stream().anyMatch(AbstractCreateIndexCommand.class::isInstance);

        if (!hasCreateIndexCommand) {
            return fut;
        }

        return fut.thenCompose(applyResult ->
                inBusyLock(busyLock, () -> {
                    List<CompletableFuture<?>> toWait = new ArrayList<>();
                    for (int i = 0; i < batch.size(); i++) {
                        CatalogCommand cmd = batch.get(i);

                        if (!(cmd instanceof AbstractCreateIndexCommand) || !applyResult.isApplied(i)) {
                            continue;
                        }

                        toWait.add(waitTillIndexBecomesAvailableOrRemoved(
                                ((AbstractCreateIndexCommand) cmd), i, applyResult
                        ));
                    }

                    return CompletableFutures.allOf(toWait).thenApply(none -> applyResult);
                })
        );
    }

    /**
     * Handles ddl commands.
     *
     * @param cmd Catalog command.
     * @return Future containing result of applying a commands to catalog.
     */
    public CompletableFuture<CatalogApplyResult> handle(CatalogCommand cmd) {
        CompletableFuture<CatalogApplyResult> fut = catalogManager.execute(cmd);

        if (cmd instanceof AbstractCreateIndexCommand) {
            fut = fut.thenCompose(applyResult ->
                    inBusyLock(busyLock, () -> waitTillIndexBecomesAvailableOrRemoved((AbstractCreateIndexCommand) cmd, 0, applyResult)));
        }

        return fut;
    }

    private CompletableFuture<CatalogApplyResult> waitTillIndexBecomesAvailableOrRemoved(
            AbstractCreateIndexCommand cmd,
            int commandIdx,
            CatalogApplyResult catalogApplyResult
    ) {
        if (!catalogApplyResult.isApplied(commandIdx)) {
            return CompletableFuture.completedFuture(catalogApplyResult);
        }

        int creationCatalogVersion = catalogApplyResult.getCatalogVersion();

        Catalog catalog = catalogManager.catalog(creationCatalogVersion);
        assert catalog != null : creationCatalogVersion;

        CatalogSchemaDescriptor schema = catalog.schema(cmd.schemaName());
        assert schema != null : "Did not find schema " + cmd.schemaName() + " in version " + creationCatalogVersion;

        CatalogIndexDescriptor index = schema.aliveIndex(cmd.indexName());
        assert index != null
                : "Did not find index " + cmd.indexName() + " in schema " + cmd.schemaName() + " in version " + creationCatalogVersion;

        // If index already has required state, no need to wait.
        if (index.status() == CatalogIndexStatus.AVAILABLE) {
            return CompletableFuture.completedFuture(catalogApplyResult);
        }

        CompletableFuture<Void> future = inFlightFutures.registerFuture(new CompletableFuture<>());

        EventListener<CatalogEventParameters> availabilityListener = EventListener.fromConsumer(event -> {
            if (((MakeIndexAvailableEventParameters) event).indexId() == index.id()) {
                completeFutureWhenEventVersionActivates(future, event);
            }
        });
        catalogManager.listen(CatalogEvent.INDEX_AVAILABLE, availabilityListener);

        EventListener<CatalogEventParameters> removalListener = EventListener.fromConsumer(event -> {
            if (((RemoveIndexEventParameters) event).indexId() == index.id()) {
                future.complete(null);
            }
        });
        catalogManager.listen(CatalogEvent.INDEX_REMOVED, removalListener);

        // We added listeners, but the index could switch to a state of interest before we added them, so check
        // explicitly.
        int latestVersion = catalogManager.latestCatalogVersion();
        for (int version = creationCatalogVersion + 1; version <= latestVersion; version++) {
            CatalogIndexDescriptor indexAtVersion = catalogManager.catalog(version).index(index.id());
            if (indexAtVersion == null) {
                // It's already removed.
                future.complete(null);
                break;
            } else if (indexAtVersion.status().isAvailableOrLater()) {
                // It was already made available.
                completeFutureWhenEventVersionActivates(future, version);
                break;
            }
        }

        return future.whenComplete((res, ex) -> {
            catalogManager.removeListener(CatalogEvent.INDEX_AVAILABLE, availabilityListener);
            catalogManager.removeListener(CatalogEvent.INDEX_REMOVED, removalListener);
        }).thenApply(none -> catalogApplyResult);
    }

    private void completeFutureWhenEventVersionActivates(CompletableFuture<Void> future, CatalogEventParameters event) {
        completeFutureWhenEventVersionActivates(future, event.catalogVersion());
    }

    private void completeFutureWhenEventVersionActivates(CompletableFuture<Void> future, int catalogVersion) {
        Catalog catalog = catalogManager.catalog(catalogVersion);
        assert catalog != null;

        HybridTimestamp tsToWait = clusterWideEnsuredActivationTimestamp(catalog.time(), clockService.maxClockSkewMillis());
        clockService.waitFor(tsToWait)
                .whenComplete((res, ex) -> future.complete(null));
    }

    @Override
    public void start() {
        // No-op.
    }

    @Override
    public void stop() throws Exception {
        busyLock.block();

        inFlightFutures.failInFlightFutures(new NodeStoppingException());
    }
}
