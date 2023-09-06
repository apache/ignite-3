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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.TableExistsValidationException;
import org.apache.ignite.internal.catalog.TableNotFoundValidationException;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterColumnCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableAddCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableDropCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropTableCommand;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;

/**
 * Wrapper for DDL command handler passes DDL commands to CatalogService.
 * TODO: IGNITE-19082 Drop this wrapper when all the versioned schema stuff will be moved from Configuration to Catalog.
 */
public class DdlCommandHandlerWrapper extends DdlCommandHandler {
    /**
     * Constructor.
     */
    public DdlCommandHandlerWrapper(TableManager tableManager, CatalogManager catalogManager) {
        super(tableManager, catalogManager);
    }

    /** Handles ddl commands. */
    @Override
    public CompletableFuture<Boolean> handle(DdlCommand cmd) {
        // Handle command in usual way.
        CompletableFuture<Boolean> ddlCommandFuture = super.handle(cmd);

        // Pass supported commands to the Catalog.
        if (cmd instanceof CreateTableCommand) {
            return ddlCommandFuture
                    .thenCompose(res -> catalogManager.execute(
                            DdlToCatalogCommandConverter.convert((CreateTableCommand) cmd))
                            .handle(handleModificationResult(
                                    ((CreateTableCommand) cmd).ifTableExists(), TableExistsValidationException.class))
                    ).handle(handleModificationResult(((CreateTableCommand) cmd).ifTableExists(), TableAlreadyExistsException.class));
        } else if (cmd instanceof DropTableCommand) {
            return ddlCommandFuture
                    .thenCompose(res -> catalogManager.execute(
                            DdlToCatalogCommandConverter.convert((DropTableCommand) cmd))
                            .handle(handleModificationResult(
                                    ((DropTableCommand) cmd).ifTableExists(), TableNotFoundValidationException.class))
                    ).handle(handleModificationResult(((DropTableCommand) cmd).ifTableExists(), TableNotFoundException.class));
        } else if (cmd instanceof AlterTableAddCommand) {
            AlterTableAddCommand addCommand = (AlterTableAddCommand) cmd;

            return ddlCommandFuture
                    .thenCompose(res -> catalogManager.addColumn(DdlToCatalogCommandConverter.convert(addCommand))
                            .handle(handleModificationResult(addCommand.ifTableExists(), TableNotFoundException.class))
                    );
        } else if (cmd instanceof AlterTableDropCommand) {
            AlterTableDropCommand dropCommand = (AlterTableDropCommand) cmd;

            return ddlCommandFuture
                    .thenCompose(res -> catalogManager.dropColumn(DdlToCatalogCommandConverter.convert(dropCommand))
                            .handle(handleModificationResult(dropCommand.ifTableExists(), TableNotFoundException.class))
                    );
        } else if (cmd instanceof AlterColumnCommand) {
            return ddlCommandFuture
                    .thenCompose(res -> catalogManager.alterColumn(DdlToCatalogCommandConverter.convert((AlterColumnCommand) cmd))
                            .handle(handleModificationResult(((AlterColumnCommand) cmd).ifTableExists(), TableNotFoundException.class))
                    );
        }

        return ddlCommandFuture;
    }
}
