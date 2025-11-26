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

package org.apache.ignite.migrationtools.tablemanagement;

import static org.apache.ignite3.catalog.definitions.ColumnDefinition.column;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite3.catalog.ColumnType;
import org.apache.ignite3.catalog.definitions.TableDefinition;
import org.apache.ignite3.client.IgniteClient;
import org.apache.ignite3.table.KeyValueView;
import org.apache.ignite3.table.Table;
import org.jetbrains.annotations.Nullable;

/** {@link TableTypeRegistry} implementation backed by a table. */
public class PersistentTableTypeRegistryImpl implements TableTypeRegistry {
    private static final String TABLE_NAME = Namespace.resolveTableName("TABLETYPEREGISTRY");

    private final IgniteClient client;

    private final CompletableFuture<KeyValueView<String, TableTypeRecord>> tableFuture;

    private final ObjectMapper jsonObjectMapper = new ObjectMapper();

    public PersistentTableTypeRegistryImpl(IgniteClient client) {
        this.client = client;
        this.tableFuture = initTable().thenApply(table -> table.keyValueView(String.class, TableTypeRecord.class));
    }

    private CompletableFuture<Table> initTable() {
        // TODO: GG-39800 Add logs
        // TODO: GG-39801 Add a private schema if AI3 when supports it.
        TableDefinition def = TableDefinition.builder(TABLE_NAME)
                .ifNotExists()
                .columns(
                        column("TABLE_KEY", ColumnType.VARCHAR),
                        column("keyClass", ColumnType.VARCHAR),
                        column("valClass", ColumnType.VARCHAR),
                        column("keyColumnMappings", ColumnType.VARCHAR.nullable(true)),
                        column("valColumnMappings", ColumnType.VARCHAR.nullable(true)))
                .primaryKey("TABLE_KEY")
                .build();

        return this.client.catalog().createTableAsync(def);
    }

    @Override
    public @Nullable TableTypeDescriptor typesForTable(String tableName) {
        var table = this.tableFuture.join();
        @Nullable TableTypeRecord types = table.get(null, tableName);
        if (types == null) {
            return null;
        }

        try {
            Map<String, String> keyColumnMappings = (types.keyColumnMappings != null)
                    ? this.jsonObjectMapper.readValue(types.keyColumnMappings, Map.class)
                    : null;

            Map<String, String> valColumnMappings = (types.valColumnMappings != null)
                    ? this.jsonObjectMapper.readValue(types.valColumnMappings, Map.class)
                    : null;

            return new TableTypeDescriptor(types.keyClass, types.valClass, keyColumnMappings, valColumnMappings);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void registerTypesForTable(String tableName, TableTypeDescriptor tableDescriptor) {
        var table = this.tableFuture.join();

        try {
            String keyColumnMappingsAsJson = (tableDescriptor.keyFieldNameForColumn() != null)
                    ? jsonObjectMapper.writeValueAsString(tableDescriptor.keyFieldNameForColumn())
                    : null;

            String valColumnMappingsAsJson = (tableDescriptor.valFieldNameForColumn() != null)
                    ? jsonObjectMapper.writeValueAsString(tableDescriptor.valFieldNameForColumn())
                    : null;

            var record = new TableTypeRecord(
                    tableDescriptor.keyClassName(),
                    tableDescriptor.valClassName(),
                    keyColumnMappingsAsJson,
                    valColumnMappingsAsJson
            );

            table.put(null, tableName, record);
        } catch (JsonProcessingException e) {
            // TODO: Figure out what to do next.
            throw new RuntimeException(e);
        }
    }

    private static class TableTypeRecord {
        private String keyClass;

        private String valClass;

        @Nullable
        private String keyColumnMappings;

        @Nullable
        private String valColumnMappings;

        public TableTypeRecord() {
            // Intentionally left blank
        }

        public TableTypeRecord(String keyClass, String valClass, @Nullable String keyColumnMappings, @Nullable String valColumnMappings) {
            this.keyClass = keyClass;
            this.valClass = valClass;
            this.keyColumnMappings = keyColumnMappings;
            this.valColumnMappings = valColumnMappings;
        }
    }
}
