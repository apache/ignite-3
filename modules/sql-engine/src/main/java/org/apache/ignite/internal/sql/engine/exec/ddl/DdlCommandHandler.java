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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.clusterWideEnsuredActivationTimestamp;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.DistributionZoneExistsValidationException;
import org.apache.ignite.internal.catalog.DistributionZoneNotFoundValidationException;
import org.apache.ignite.internal.catalog.IndexExistsValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.SchemaExistsException;
import org.apache.ignite.internal.catalog.SchemaNotFoundException;
import org.apache.ignite.internal.catalog.TableExistsValidationException;
import org.apache.ignite.internal.catalog.TableNotFoundValidationException;
import org.apache.ignite.internal.catalog.commands.AbstractCreateIndexCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableAlterColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneSetDefaultCommand;
import org.apache.ignite.internal.catalog.commands.CreateSchemaCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.DropIndexCommand;
import org.apache.ignite.internal.catalog.commands.DropSchemaCommand;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.commands.DropZoneCommand;
import org.apache.ignite.internal.catalog.commands.RenameZoneCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
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
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

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
     * Handles ddl commands.
     *
     * @param cmd Catalog command.
     * @return Future representing pending completion of the operation. If the command execution resulted in a modification of the catalog,
     *         the result will be the activation timestamp of the new catalog version, if the command did not result in a change of the
     *         catalog, the result will be {@code null}.
     */
    public CompletableFuture<@Nullable Long> handle(CatalogCommand cmd) {
        if (cmd instanceof CreateSchemaCommand) {
            return handleCreateSchema((CreateSchemaCommand) cmd);
        } else if (cmd instanceof DropSchemaCommand) {
            return handleDropSchema((DropSchemaCommand) cmd);
        } else if (cmd instanceof CreateTableCommand) {
            return handleCreateTable((CreateTableCommand) cmd);
        } else if (cmd instanceof DropTableCommand) {
            return handleDropTable((DropTableCommand) cmd);
        } else if (cmd instanceof AlterTableAddColumnCommand) {
            return handleAlterAddColumn((AlterTableAddColumnCommand) cmd);
        } else if (cmd instanceof AlterTableDropColumnCommand) {
            return handleAlterDropColumn((AlterTableDropColumnCommand) cmd);
        } else if (cmd instanceof AlterTableAlterColumnCommand) {
            return handleAlterColumn((AlterTableAlterColumnCommand) cmd);
        } else if (cmd instanceof AbstractCreateIndexCommand) {
            return handleCreateIndex((AbstractCreateIndexCommand) cmd);
        } else if (cmd instanceof DropIndexCommand) {
            return handleDropIndex((DropIndexCommand) cmd);
        } else if (cmd instanceof CreateZoneCommand) {
            return handleCreateZone((CreateZoneCommand) cmd);
        } else if (cmd instanceof RenameZoneCommand) {
            return handleRenameZone((RenameZoneCommand) cmd);
        } else if (cmd instanceof AlterZoneCommand) {
            return handleAlterZone((AlterZoneCommand) cmd);
        } else if (cmd instanceof AlterZoneSetDefaultCommand) {
            return handleAlterZoneSetDefault((AlterZoneSetDefaultCommand) cmd);
        } else if (cmd instanceof DropZoneCommand) {
            return handleDropZone((DropZoneCommand) cmd);
        } else {
            return failedFuture(new SqlException(STMT_VALIDATION_ERR, "Unsupported DDL operation ["
                    + "cmdName=" + (cmd == null ? null : cmd.getClass().getSimpleName()) + "; "
                    + "cmd=\"" + cmd + "\"]"));
        }
    }

    /** Handles create distribution zone command. */
    private CompletableFuture<@Nullable Long> handleCreateZone(CreateZoneCommand cmd) {
        return catalogManager.execute(cmd)
                .handle(handleModificationResult(cmd.ifNotExists(), DistributionZoneExistsValidationException.class));
    }

    /** Handles rename zone command. */
    private CompletableFuture<@Nullable Long> handleRenameZone(RenameZoneCommand cmd) {
        return catalogManager.execute(cmd)
                .handle(handleModificationResult(cmd.ifExists(), DistributionZoneNotFoundValidationException.class));
    }

    /** Handles alter zone command. */
    private CompletableFuture<@Nullable Long> handleAlterZone(AlterZoneCommand cmd) {
        return catalogManager.execute(cmd)
                .handle(handleModificationResult(cmd.ifExists(), DistributionZoneNotFoundValidationException.class));
    }

    /** Handles alter zone set default command. */
    private CompletableFuture<@Nullable Long> handleAlterZoneSetDefault(AlterZoneSetDefaultCommand cmd) {
        return catalogManager.execute(cmd)
                .handle(handleModificationResult(cmd.ifExists(), DistributionZoneNotFoundValidationException.class));
    }

    /** Handles drop distribution zone command. */
    private CompletableFuture<@Nullable Long> handleDropZone(DropZoneCommand cmd) {
        return catalogManager.execute(cmd)
                .handle(handleModificationResult(cmd.ifExists(), DistributionZoneNotFoundValidationException.class));
    }

    /** Handles create table command. */
    private CompletableFuture<@Nullable Long> handleCreateTable(CreateTableCommand cmd) {
        return catalogManager.execute(cmd)
                .handle(handleModificationResult(cmd.ifTableExists(), TableExistsValidationException.class));
    }

    /** Handles drop table command. */
    private CompletableFuture<@Nullable Long> handleDropTable(DropTableCommand cmd) {
        return catalogManager.execute(cmd)
                .handle(handleModificationResult(cmd.ifTableExists(), TableNotFoundValidationException.class));
    }

    /** Handles add column command. */
    private CompletableFuture<@Nullable Long> handleAlterAddColumn(AlterTableAddColumnCommand cmd) {
        return catalogManager.execute(cmd)
                .handle(handleModificationResult(cmd.ifTableExists(), TableNotFoundValidationException.class));
    }

    /** Handles drop column command. */
    private CompletableFuture<@Nullable Long> handleAlterDropColumn(AlterTableDropColumnCommand cmd) {
        return catalogManager.execute(cmd)
                .handle(handleModificationResult(cmd.ifTableExists(), TableNotFoundValidationException.class));
    }

    /** Handles drop column command. */
    private CompletableFuture<@Nullable Long> handleAlterColumn(AlterTableAlterColumnCommand cmd) {
        return catalogManager.execute(cmd)
                .handle(handleModificationResult(cmd.ifTableExists(), TableNotFoundValidationException.class));
    }

    /** Handles create index command. */
    private CompletableFuture<@Nullable Long> handleCreateIndex(AbstractCreateIndexCommand cmd) {
        return catalogManager.execute(cmd)
                .thenCompose(catalogVersion -> inBusyLock(busyLock, () -> waitTillIndexBecomesAvailableOrRemoved(cmd, catalogVersion)))
                .handle(handleModificationResult(cmd.ifNotExists(), IndexExistsValidationException.class));
    }

    /** Handles drop index command. */
    private CompletableFuture<@Nullable Long> handleDropIndex(DropIndexCommand cmd) {
        return catalogManager.execute(cmd)
                .handle(handleModificationResult(cmd.ifExists(), IndexNotFoundValidationException.class));
    }

    /** Handles create schema command. */
    private CompletableFuture<Long> handleCreateSchema(CreateSchemaCommand cmd) {
        return catalogManager.execute(cmd)
                .handle(handleModificationResult(cmd.ifNotExists(), SchemaExistsException.class));
    }

    /** Handles drop schema command. */
    private CompletableFuture<Long> handleDropSchema(DropSchemaCommand cmd) {
        return catalogManager.execute(cmd)
                .handle(handleModificationResult(cmd.ifExists(), SchemaNotFoundException.class));
    }

    private BiFunction<Integer, Throwable, @Nullable Long> handleModificationResult(boolean ignoreExpectedError, Class<?> expErrCls) {
        return (ver, err) -> {
            if (err == null) {
                Catalog catalog = catalogManager.catalog(ver);

                assert catalog != null;

                return catalog.time();
            } else if (ignoreExpectedError) {
                Throwable err0 = err instanceof CompletionException ? err.getCause() : err;

                if (expErrCls.isAssignableFrom(err0.getClass())) {
                    return null;
                }
            }

            throw (err instanceof RuntimeException) ? (RuntimeException) err : new CompletionException(err);
        };
    }

    private CompletionStage<Integer> waitTillIndexBecomesAvailableOrRemoved(
            AbstractCreateIndexCommand cmd,
            Integer creationCatalogVersion
    ) {
        CompletableFuture<Void> future = inFlightFutures.registerFuture(new CompletableFuture<>());

        Catalog catalog = catalogManager.catalog(creationCatalogVersion);
        assert catalog != null : creationCatalogVersion;

        CatalogSchemaDescriptor schema = catalog.schema(cmd.schemaName());
        assert schema != null : "Did not find schema " + cmd.schemaName() + " in version " + creationCatalogVersion;

        CatalogIndexDescriptor index = schema.aliveIndex(cmd.indexName());
        assert index != null
                : "Did not find index " + cmd.indexName() + " in schema " + cmd.schemaName() + " in version " + creationCatalogVersion;

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
        }).thenApply(none -> creationCatalogVersion);
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
