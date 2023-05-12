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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropTableCommand;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;

/**
 * Wrapper for DDL command handler passes DDL commands to CatalogService.
 * TODO: IGNITE-19082 Drop this class when all versioned schema stuff will be moved to Catalog.
 */
public class DdlCommandHandlerWrapper extends DdlCommandHandler {

    private final CatalogManager catalogManager;

    /**
     * Constructor.
     */
    public DdlCommandHandlerWrapper(
            DistributionZoneManager distributionZoneManager,
            TableManager tableManager,
            IndexManager indexManager,
            DataStorageManager dataStorageManager,
            CatalogManager catalogManager
    ) {
        super(distributionZoneManager, tableManager, indexManager, dataStorageManager);

        this.catalogManager = Objects.requireNonNull(catalogManager, "Catalog service");
    }

    /** Handles ddl commands. */
    @Override
    public CompletableFuture<Boolean> handle(DdlCommand cmd) {
        if (cmd instanceof CreateTableCommand) {
            return super.handle(cmd)
                    .thenCompose(res -> catalogManager.createTable(DdlToCatalogCommandConverter.convert((CreateTableCommand) cmd))
                            .handle(handleModificationResult(((CreateTableCommand) cmd).ifTableExists(), TableAlreadyExistsException.class))
                    );
        } else if (cmd instanceof DropTableCommand) {
            return super.handle(cmd)
                    .thenCompose(res -> catalogManager.dropTable(DdlToCatalogCommandConverter.convert((DropTableCommand) cmd))
                            .handle(handleModificationResult(((DropTableCommand) cmd).ifTableExists(), TableNotFoundException.class))
                    );
        }

        return super.handle(cmd);
    }
}
