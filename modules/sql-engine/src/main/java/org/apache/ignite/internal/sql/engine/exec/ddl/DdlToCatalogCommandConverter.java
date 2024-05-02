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

import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.parseStorageProfiles;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableAlterColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableAlterColumnCommandBuilder;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneSetDefaultCatalogCommand;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexCommand;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.RenameZoneCommand;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.commands.TablePrimaryKey;
import org.apache.ignite.internal.catalog.commands.TableSortedPrimaryKey;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterColumnCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableAddCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableDropCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneRenameCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneSetCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneSetDefaultCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.ColumnDefinition;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateIndexCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateTableCommand.PrimaryKeyIndexType;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateZoneCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DefaultValueDefinition;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropIndexCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropTableCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropZoneCommandToRemove;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.sql.ColumnType;

/**
 * Converter for DDL command classes to Catalog command params classes.
 */
class DdlToCatalogCommandConverter {
    static CatalogCommand convert(CreateTableCommand cmd) {
        List<ColumnParams> columns = cmd.columns().stream().map(DdlToCatalogCommandConverter::convert).collect(Collectors.toList());

        PrimaryKeyIndexType pkIndexType = cmd.primaryIndexType();
        TablePrimaryKey primaryKey;

        switch (pkIndexType) {
            case SORTED:
                List<CatalogColumnCollation> collations = cmd.primaryKeyCollations().stream()
                        .map(DdlToCatalogCommandConverter::convert)
                        .collect(Collectors.toList());

                primaryKey = TableSortedPrimaryKey.builder()
                        .columns(cmd.primaryKeyColumns())
                        .collations(collations)
                        .build();
                break;
            case HASH:
                primaryKey = TableHashPrimaryKey.builder()
                        .columns(cmd.primaryKeyColumns())
                        .build();
                break;
            default:
                throw new IllegalArgumentException("Unexpected primary key index type: " + pkIndexType);
        }

        return org.apache.ignite.internal.catalog.commands.CreateTableCommand.builder()
                .schemaName(cmd.schemaName())
                .tableName(cmd.tableName())

                .columns(columns)
                .primaryKey(primaryKey)
                .colocationColumns(cmd.colocationColumns())

                .zone(cmd.zone())
                .storageProfile(cmd.storageProfile())

                .build();
    }

    static CatalogCommand convert(DropTableCommand cmd) {
        return org.apache.ignite.internal.catalog.commands.DropTableCommand.builder()
                .schemaName(cmd.schemaName())
                .tableName(cmd.tableName())
                .build();
    }

    static CatalogCommand convert(CreateZoneCommand cmd) {
        return org.apache.ignite.internal.catalog.commands.CreateZoneCommand.builder()
                .zoneName(cmd.zoneName())
                .partitions(cmd.partitions())
                .replicas(cmd.replicas())
                .filter(cmd.nodeFilter())
                .dataNodesAutoAdjust(cmd.dataNodesAutoAdjust())
                .dataNodesAutoAdjustScaleUp(cmd.dataNodesAutoAdjustScaleUp())
                .dataNodesAutoAdjustScaleDown(cmd.dataNodesAutoAdjustScaleDown())
                .storageProfilesParams(parseStorageProfiles(cmd.storageProfiles()))
                .build();
    }

    static CatalogCommand convert(DropZoneCommandToRemove cmd) {
        return org.apache.ignite.internal.catalog.commands.DropZoneCommand.builder()
                .zoneName(cmd.zoneName())
                .build();
    }

    static CatalogCommand convert(AlterZoneRenameCommand cmd) {
        return RenameZoneCommand.builder()
                .zoneName(cmd.zoneName())
                .newZoneName(cmd.newZoneName())
                .build();
    }

    static CatalogCommand convert(AlterZoneSetCommand cmd) {
        return AlterZoneCommand.builder()
                .zoneName(cmd.zoneName())
                .partitions(cmd.partitions())
                .replicas(cmd.replicas())
                .filter(cmd.nodeFilter())
                .dataNodesAutoAdjust(cmd.dataNodesAutoAdjust())
                .dataNodesAutoAdjustScaleUp(cmd.dataNodesAutoAdjustScaleUp())
                .dataNodesAutoAdjustScaleDown(cmd.dataNodesAutoAdjustScaleDown())
                .build();
    }

    static CatalogCommand convert(AlterZoneSetDefaultCommand cmd) {
        return AlterZoneSetDefaultCatalogCommand.builder()
                .zoneName(cmd.zoneName())
                .build();
    }

    static CatalogCommand convert(AlterColumnCommand cmd) {
        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder()
                .schemaName(cmd.schemaName())
                .tableName(cmd.tableName())
                .columnName(cmd.columnName());

        Boolean notNull = cmd.notNull();
        if (notNull != null) {
            builder.nullable(!notNull);
        }

        Function<ColumnType, DefaultValue> defaultValueResolver = cmd.defaultValueResolver();
        if (defaultValueResolver != null) {
            builder.deferredDefaultValue(defaultValueResolver::apply);
        }

        RelDataType type = cmd.type();

        if (type != null) {
            builder.type(TypeUtils.columnType(type));

            if (type.getPrecision() != RelDataType.PRECISION_NOT_SPECIFIED) {
                if (type.getSqlTypeName() == SqlTypeName.VARCHAR || type.getSqlTypeName() == SqlTypeName.VARBINARY) {
                    builder.length(type.getPrecision());
                } else {
                    builder.precision(type.getPrecision());
                }
            }

            if (type.getScale() != RelDataType.SCALE_NOT_SPECIFIED) {
                builder.scale(type.getScale());
            }
        }

        return builder.build();
    }

    static CatalogCommand convert(AlterTableAddCommand cmd) {
        List<ColumnParams> columns = cmd.columns().stream().map(DdlToCatalogCommandConverter::convert).collect(Collectors.toList());

        return AlterTableAddColumnCommand.builder()
                .schemaName(cmd.schemaName())
                .tableName(cmd.tableName())

                .columns(columns)

                .build();
    }

    static CatalogCommand convert(AlterTableDropCommand cmd) {
        return AlterTableDropColumnCommand.builder()
                .schemaName(cmd.schemaName())
                .tableName(cmd.tableName())

                .columns(cmd.columns())

                .build();
    }


    static CatalogCommand convert(CreateIndexCommand cmd) {
        switch (cmd.type()) {
            case HASH:
                return CreateHashIndexCommand.builder()
                        .schemaName(cmd.schemaName())
                        .indexName(cmd.indexName())

                        .tableName(cmd.tableName())
                        .columns(cmd.columns())

                        .build();
            case SORTED:
                List<CatalogColumnCollation> collations = cmd.collations().stream()
                        .map(DdlToCatalogCommandConverter::convert)
                        .collect(Collectors.toList());

                return CreateSortedIndexCommand.builder()
                        .schemaName(cmd.schemaName())
                        .indexName(cmd.indexName())

                        .tableName(cmd.tableName())
                        .columns(cmd.columns())
                        .collations(collations)

                        .build();
            default:
                throw new IllegalArgumentException("Unsupported index type: " + cmd.type());
        }
    }

    static CatalogCommand convert(DropIndexCommand cmd) {
        return org.apache.ignite.internal.catalog.commands.DropIndexCommand.builder()
                .schemaName(cmd.schemaName())
                .indexName(cmd.indexName())
                .build();
    }

    private static ColumnParams convert(ColumnDefinition def) {
        return ColumnParams.builder()
                .name(def.name())
                .type(TypeUtils.columnType(def.type()))
                .nullable(def.nullable())
                .precision(def.precision())
                .scale(def.scale())
                .length(def.length())
                .defaultValue(convert(def.defaultValueDefinition()))
                .build();
    }

    private static DefaultValue convert(DefaultValueDefinition def) {
        switch (def.type()) {
            case CONSTANT:
                return DefaultValue.constant(((DefaultValueDefinition.ConstantValue) def).value());

            case FUNCTION_CALL:
                return DefaultValue.functionCall(((DefaultValueDefinition.FunctionCall) def).functionName());

            default:
                throw new IllegalArgumentException("Default value definition: " + def.type());
        }
    }

    private static CatalogColumnCollation convert(IgniteIndex.Collation collation) {
        return CatalogColumnCollation.get(collation.asc, collation.nullsFirst);
    }
}
