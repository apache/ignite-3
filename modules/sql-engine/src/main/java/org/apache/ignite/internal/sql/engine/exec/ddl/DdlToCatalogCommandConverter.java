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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.commands.AbstractIndexCommandParams;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DropIndexParams;
import org.apache.ignite.internal.catalog.commands.DropTableParams;
import org.apache.ignite.internal.catalog.descriptors.ColumnCollation;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableAddCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableDropCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.ColumnDefinition;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateIndexCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DefaultValueDefinition;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropIndexCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropTableCommand;
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
                List<ColumnCollation> collations = cmd.collations().stream()
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

    private static ColumnCollation convert(IgniteIndex.Collation collation) {
        return ColumnCollation.get(collation.asc, collation.nullsFirst);
    }
}
