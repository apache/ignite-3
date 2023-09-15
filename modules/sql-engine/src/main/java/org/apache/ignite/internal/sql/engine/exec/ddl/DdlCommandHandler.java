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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.IndexExistsValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.TableExistsValidationException;
import org.apache.ignite.internal.catalog.TableNotFoundValidationException;
import org.apache.ignite.internal.distributionzones.DistributionZoneAlreadyExistsException;
import org.apache.ignite.internal.distributionzones.DistributionZoneNotFoundException;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterColumnCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableAddCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableDropCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneRenameCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneSetCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateIndexCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateZoneCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropIndexCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropTableCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropZoneCommand;
import org.apache.ignite.sql.SqlException;

/** DDL commands handler. */
public class DdlCommandHandler {
    private final CatalogManager catalogManager;

    /**
     * Constructor.
     */
    public DdlCommandHandler(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    /** Handles ddl commands. */
    @WithSpan
    public CompletableFuture<Boolean> handle(DdlCommand cmd) {
        if (cmd instanceof CreateTableCommand) {
            return handleCreateTable((CreateTableCommand) cmd);
        } else if (cmd instanceof DropTableCommand) {
            return handleDropTable((DropTableCommand) cmd);
        } else if (cmd instanceof AlterTableAddCommand) {
            return handleAlterAddColumn((AlterTableAddCommand) cmd);
        } else if (cmd instanceof AlterTableDropCommand) {
            return handleAlterDropColumn((AlterTableDropCommand) cmd);
        } else if (cmd instanceof AlterColumnCommand) {
            return handleAlterColumn((AlterColumnCommand) cmd);
        } else if (cmd instanceof CreateIndexCommand) {
            return handleCreateIndex((CreateIndexCommand) cmd);
        } else if (cmd instanceof DropIndexCommand) {
            return handleDropIndex((DropIndexCommand) cmd);
        } else if (cmd instanceof CreateZoneCommand) {
            return handleCreateZone((CreateZoneCommand) cmd);
        } else if (cmd instanceof AlterZoneRenameCommand) {
            return handleRenameZone((AlterZoneRenameCommand) cmd);
        } else if (cmd instanceof AlterZoneSetCommand) {
            return handleAlterZone((AlterZoneSetCommand) cmd);
        } else if (cmd instanceof DropZoneCommand) {
            return handleDropZone((DropZoneCommand) cmd);
        } else {
            return failedFuture(new SqlException(STMT_VALIDATION_ERR, "Unsupported DDL operation ["
                    + "cmdName=" + (cmd == null ? null : cmd.getClass().getSimpleName()) + "; "
                    + "cmd=\"" + cmd + "\"]"));
        }
    }

    /** Handles create distribution zone command. */
    @WithSpan
    private CompletableFuture<Boolean> handleCreateZone(CreateZoneCommand cmd) {
        return catalogManager.createZone(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifNotExists(), DistributionZoneAlreadyExistsException.class));
    }

    /** Handles rename zone command. */
    @WithSpan
    private CompletableFuture<Boolean> handleRenameZone(AlterZoneRenameCommand cmd) {
        return catalogManager.renameZone(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifExists(), DistributionZoneNotFoundException.class));
    }

    /** Handles alter zone command. */
    @WithSpan
    private CompletableFuture<Boolean> handleAlterZone(AlterZoneSetCommand cmd) {
        return catalogManager.alterZone(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifExists(), DistributionZoneNotFoundException.class));
    }

    /** Handles drop distribution zone command. */
    @WithSpan
    private CompletableFuture<Boolean> handleDropZone(DropZoneCommand cmd) {
        return catalogManager.dropZone(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifExists(), DistributionZoneNotFoundException.class));
    }

    /** Handles create table command. */
    @WithSpan
    private CompletableFuture<Boolean> handleCreateTable(CreateTableCommand cmd) {
        return catalogManager.execute(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifTableExists(), TableExistsValidationException.class));
    }

    /** Handles drop table command. */
    @WithSpan
    private CompletableFuture<Boolean> handleDropTable(DropTableCommand cmd) {
        return catalogManager.execute(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifTableExists(), TableNotFoundValidationException.class));
    }

    /** Handles add column command. */
    @WithSpan
    private CompletableFuture<Boolean> handleAlterAddColumn(AlterTableAddCommand cmd) {
        if (nullOrEmpty(cmd.columns())) {
            return completedFuture(Boolean.FALSE);
        }

        return catalogManager.execute(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifTableExists(), TableNotFoundValidationException.class));
    }

    /** Handles drop column command. */
    @WithSpan
    private CompletableFuture<Boolean> handleAlterDropColumn(AlterTableDropCommand cmd) {
        if (nullOrEmpty(cmd.columns())) {
            return completedFuture(Boolean.FALSE);
        }

        return catalogManager.execute(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifTableExists(), TableNotFoundValidationException.class));
    }

    /** Handles drop column command. */
    private CompletableFuture<Boolean> handleAlterColumn(AlterColumnCommand cmd) {
        return catalogManager.execute(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifTableExists(), TableNotFoundValidationException.class));
    }

    private static BiFunction<Object, Throwable, Boolean> handleModificationResult(boolean ignoreExpectedError, Class<?> expErrCls) {
        return (val, err) -> {
            if (err == null) {
                return val instanceof Boolean ? (Boolean) val : Boolean.TRUE;
            } else if (ignoreExpectedError) {
                Throwable err0 = err instanceof CompletionException ? err.getCause() : err;

                if (expErrCls.isAssignableFrom(err0.getClass())) {
                    return Boolean.FALSE;
                }
            }

            throw (err instanceof RuntimeException) ? (RuntimeException) err : new CompletionException(err);
        };
    }

    /** Handles create index command. */
    @WithSpan
    private CompletableFuture<Boolean> handleCreateIndex(CreateIndexCommand cmd) {
        return catalogManager.execute(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifNotExists(), IndexExistsValidationException.class));
    }

    /** Handles drop index command. */
    @WithSpan
    private CompletableFuture<Boolean> handleDropIndex(DropIndexCommand cmd) {
        return catalogManager.execute(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifNotExists(), IndexNotFoundValidationException.class));
    }
}
