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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_DATA_REGION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_STORAGE_ENGINE;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.catalog.commands.AbstractIndexCommandParams;
import org.apache.ignite.internal.catalog.commands.AlterColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterZoneParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.catalog.commands.DataStorageParams;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DropIndexParams;
import org.apache.ignite.internal.catalog.commands.DropTableParams;
import org.apache.ignite.internal.catalog.commands.DropZoneParams;
import org.apache.ignite.internal.catalog.commands.RenameZoneParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterColumnCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableAddCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableDropCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneRenameCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneSetCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.ColumnDefinition;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateIndexCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateZoneCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DefaultValueDefinition;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropIndexCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropTableCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropZoneCommand;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;

/**
 * Converter for DDL command classes to Catalog command params classes.
 */
class DdlToCatalogCommandConverter {
    static CreateTableParams convert(CreateTableCommand cmd) {
        List<ColumnParams> columns = cmd.columns().stream().map(DdlToCatalogCommandConverter::convert).collect(Collectors.toList());

        return CreateTableParams.builder()
                .schemaName(cmd.schemaName())
                .tableName(cmd.tableName())

                .columns(columns)
                .colocationColumns(cmd.colocationColumns())
                .primaryKeyColumns(cmd.primaryKeyColumns())

                .zone(cmd.zone())

                .build();
    }

    static DropTableParams convert(DropTableCommand cmd) {
        return DropTableParams.builder()
                .schemaName(cmd.schemaName())
                .tableName(cmd.tableName())
                .build();
    }

    static CreateZoneParams convert(CreateZoneCommand cmd) {
        // TODO: IGNITE-19719 Should be defined differently
        String engine = Objects.requireNonNullElse(cmd.dataStorage(), DEFAULT_STORAGE_ENGINE);
        // TODO: IGNITE-20114 проверить позже на название параметра и не забыть про расширение тестов
        // TODO: IGNITE-19719 Must be storage engine specific
        String dataRegion = (String) cmd.dataStorageOptions().getOrDefault("dataRegion", DEFAULT_DATA_REGION);

        return CreateZoneParams.builder()
                .zoneName(cmd.zoneName())
                .partitions(cmd.partitions())
                .replicas(cmd.replicas())
                .filter(cmd.nodeFilter())
                .dataNodesAutoAdjust(cmd.dataNodesAutoAdjust())
                .dataNodesAutoAdjustScaleUp(cmd.dataNodesAutoAdjustScaleUp())
                .dataNodesAutoAdjustScaleDown(cmd.dataNodesAutoAdjustScaleDown())
                .dataStorage(DataStorageParams.builder().engine(engine).dataRegion(dataRegion).build())
                .build();
    }

    static DropZoneParams convert(DropZoneCommand cmd) {
        return DropZoneParams.builder()
                .zoneName(cmd.zoneName())
                .build();
    }

    static RenameZoneParams convert(AlterZoneRenameCommand cmd) {
        return RenameZoneParams.builder()
                .zoneName(cmd.zoneName())

                .newZoneName(cmd.newZoneName())

                .build();
    }

    static AlterZoneParams convert(AlterZoneSetCommand cmd) {
        return AlterZoneParams.builder()
                .zoneName(cmd.zoneName())

                .partitions(cmd.partitions())
                .replicas(cmd.replicas())
                .filter(cmd.nodeFilter())

                .dataNodesAutoAdjust(cmd.dataNodesAutoAdjust())
                .dataNodesAutoAdjustScaleUp(cmd.dataNodesAutoAdjustScaleUp())
                .dataNodesAutoAdjustScaleDown(cmd.dataNodesAutoAdjustScaleDown())

                .build();
    }

    static AlterColumnParams convert(AlterColumnCommand cmd) {
        AlterColumnParams.Builder builder = AlterColumnParams.builder()
                .schemaName(cmd.schemaName())
                .tableName(cmd.tableName())
                .columnName(cmd.columnName())
                .notNull(cmd.notNull())
                .defaultValueResolver(cmd.defaultValueResolver());

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

    static AlterTableAddColumnParams convert(AlterTableAddCommand cmd) {
        List<ColumnParams> columns = cmd.columns().stream().map(DdlToCatalogCommandConverter::convert).collect(Collectors.toList());

        return AlterTableAddColumnParams.builder()
                .schemaName(cmd.schemaName())
                .tableName(cmd.tableName())

                .columns(columns)

                .build();
    }

    static AlterTableDropColumnParams convert(AlterTableDropCommand cmd) {
        return AlterTableDropColumnParams.builder()
                .schemaName(cmd.schemaName())
                .tableName(cmd.tableName())

                .columns(cmd.columns())

                .build();
    }


    static AbstractIndexCommandParams convert(CreateIndexCommand cmd) {
        switch (cmd.type()) {
            case HASH:
                return CreateHashIndexParams.builder()
                        .schemaName(cmd.schemaName())
                        .indexName(cmd.indexName())

                        .tableName(cmd.tableName())
                        .columns(cmd.columns())

                        .build();
            case SORTED:
                List<CatalogColumnCollation> collations = cmd.collations().stream()
                        .map(DdlToCatalogCommandConverter::convert)
                        .collect(Collectors.toList());

                return CreateSortedIndexParams.builder()
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

    static DropIndexParams convert(DropIndexCommand cmd) {
        return DropIndexParams.builder()
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
                .length(def.precision())
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
