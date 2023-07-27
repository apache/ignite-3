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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AbstractTableDdlCommand;
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
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.lang.DistributionZoneAlreadyExistsException;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.sql.SqlException;

/** DDL commands handler. */
public class DdlCommandHandler {
    private final DistributionZoneManager distributionZoneManager;

    private final TableManager tableManager;

    private final IndexManager indexManager;

    private final DataStorageManager dataStorageManager;

    private final CatalogManager catalogManager;

    /**
     * Constructor.
     */
    public DdlCommandHandler(
            DistributionZoneManager distributionZoneManager,
            TableManager tableManager,
            IndexManager indexManager,
            DataStorageManager dataStorageManager,
            CatalogManager catalogManager
    ) {
        this.distributionZoneManager = distributionZoneManager;
        this.tableManager = tableManager;
        this.indexManager = indexManager;
        this.dataStorageManager = dataStorageManager;
        this.catalogManager = catalogManager;
    }

    /** Handles ddl commands. */
    public CompletableFuture<Boolean> handle(DdlCommand cmd) {
        validateCommand(cmd);

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

    /** Validate command. */
    private void validateCommand(DdlCommand cmd) {
        if (cmd instanceof AbstractTableDdlCommand) {
            AbstractTableDdlCommand cmd0 = (AbstractTableDdlCommand) cmd;

            if (StringUtils.nullOrEmpty(cmd0.tableName())) {
                throw new IllegalArgumentException("Table name is undefined.");
            }
        }
    }

    /** Handles create distribution zone command. */
    private CompletableFuture<Boolean> handleCreateZone(CreateZoneCommand cmd) {
        DistributionZoneConfigurationParameters.Builder zoneCfgBuilder =
                new DistributionZoneConfigurationParameters.Builder(cmd.zoneName());

        if (cmd.dataNodesAutoAdjust() != null) {
            zoneCfgBuilder.dataNodesAutoAdjust(cmd.dataNodesAutoAdjust());
        }

        if (cmd.dataNodesAutoAdjustScaleUp() != null) {
            zoneCfgBuilder.dataNodesAutoAdjustScaleUp(cmd.dataNodesAutoAdjustScaleUp());
        }

        if (cmd.dataNodesAutoAdjustScaleDown() != null) {
            zoneCfgBuilder.dataNodesAutoAdjustScaleDown(cmd.dataNodesAutoAdjustScaleDown());
        }

        if (cmd.replicas() != null) {
            zoneCfgBuilder.replicas(cmd.replicas());
        }

        if (cmd.partitions() != null) {
            zoneCfgBuilder.partitions(cmd.partitions());
        }

        if (cmd.nodeFilter() != null) {
            zoneCfgBuilder.filter(cmd.nodeFilter());
        }

        zoneCfgBuilder.dataStorageChangeConsumer(
                dataStorageManager.zoneDataStorageConsumer(cmd.dataStorage(), cmd.dataStorageOptions()));

        return distributionZoneManager.createZone(zoneCfgBuilder.build())
                .handle(handleModificationResult(cmd.ifNotExists(), DistributionZoneAlreadyExistsException.class));
    }


    /** Handles rename zone command. */
    private CompletableFuture<Boolean> handleRenameZone(AlterZoneRenameCommand cmd) {
        DistributionZoneConfigurationParameters.Builder zoneCfgBuilder =
                new DistributionZoneConfigurationParameters.Builder(cmd.newZoneName());

        boolean ifExists = cmd.ifExists();

        return distributionZoneManager.alterZone(cmd.zoneName(), zoneCfgBuilder.build())
                .handle(handleModificationResult(ifExists, DistributionZoneNotFoundException.class));
    }

    /** Handles alter zone command. */
    private CompletableFuture<Boolean> handleAlterZone(AlterZoneSetCommand cmd) {
        DistributionZoneConfigurationParameters.Builder zoneCfgBuilder =
                new DistributionZoneConfigurationParameters.Builder(cmd.zoneName());

        if (cmd.dataNodesAutoAdjustScaleDown() != null) {
            zoneCfgBuilder.dataNodesAutoAdjustScaleDown(cmd.dataNodesAutoAdjustScaleDown());
        }

        if (cmd.dataNodesAutoAdjust() != null) {
            zoneCfgBuilder.dataNodesAutoAdjust(cmd.dataNodesAutoAdjust());
        }

        if (cmd.dataNodesAutoAdjustScaleUp() != null) {
            zoneCfgBuilder.dataNodesAutoAdjustScaleUp(cmd.dataNodesAutoAdjustScaleUp());
        }

        if (cmd.replicas() != null) {
            zoneCfgBuilder.replicas(cmd.replicas());
        }

        if (cmd.partitions() != null) {
            zoneCfgBuilder.partitions(cmd.partitions());
        }

        if (cmd.nodeFilter() != null) {
            zoneCfgBuilder.filter(cmd.nodeFilter());
        }

        return distributionZoneManager.alterZone(cmd.zoneName(), zoneCfgBuilder.build())
                .handle(handleModificationResult(cmd.ifExists(), DistributionZoneNotFoundException.class));
    }

    /** Handles drop distribution zone command. */
    private CompletableFuture<Boolean> handleDropZone(DropZoneCommand cmd) {
        return distributionZoneManager.dropZone(cmd.zoneName())
                .handle(handleModificationResult(cmd.ifExists(), DistributionZoneNotFoundException.class));
    }

    /** Handles create table command. */
    private CompletableFuture<Boolean> handleCreateTable(CreateTableCommand cmd) {
        return tableManager.createTableAsync(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifTableExists(), TableAlreadyExistsException.class));
    }

    /** Handles drop table command. */
    private CompletableFuture<Boolean> handleDropTable(DropTableCommand cmd) {
        return tableManager.dropTableAsync(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifTableExists(), TableNotFoundException.class));
    }

    /** Handles add column command. */
    private CompletableFuture<Boolean> handleAlterAddColumn(AlterTableAddCommand cmd) {
        if (nullOrEmpty(cmd.columns())) {
            return completedFuture(Boolean.FALSE);
        }

        return tableManager.alterTableAddColumnAsync(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifTableExists(), TableNotFoundException.class));
    }

    /** Handles drop column command. */
    private CompletableFuture<Boolean> handleAlterDropColumn(AlterTableDropCommand cmd) {
        if (nullOrEmpty(cmd.columns())) {
            return completedFuture(Boolean.FALSE);
        }

        return tableManager.alterTableDropColumnAsync(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifTableExists(), TableNotFoundException.class));
    }

    /** Handles alter column command. */
    private CompletableFuture<Boolean> handleAlterColumn(AlterColumnCommand cmd) {
        return catalogManager.alterColumn(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifTableExists(), TableNotFoundException.class));
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
    private CompletableFuture<Boolean> handleCreateIndex(CreateIndexCommand cmd) {
        return indexManager.createIndexAsync(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifNotExists(), IndexAlreadyExistsException.class));
    }

    /** Handles drop index command. */
    private CompletableFuture<Boolean> handleDropIndex(DropIndexCommand cmd) {
        return indexManager.dropIndexAsync(DdlToCatalogCommandConverter.convert(cmd))
                .handle(handleModificationResult(cmd.ifNotExists(), IndexNotFoundException.class));
    }
}
