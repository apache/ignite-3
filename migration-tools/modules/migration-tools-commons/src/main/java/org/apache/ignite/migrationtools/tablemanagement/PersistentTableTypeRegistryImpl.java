/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.tablemanagement;

import static org.apache.ignite3.catalog.definitions.ColumnDefinition.column;

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
                        column("valClass", ColumnType.VARCHAR))
                .primaryKey("TABLE_KEY")
                .build();

        return this.client.catalog().createTableAsync(def);
    }

    @Override
    public @Nullable Map.Entry<Class<?>, Class<?>> typesForTable(String tableName) throws ClassNotFoundException {
        var table = this.tableFuture.join();
        var types = table.get(null, tableName);

        if (types == null) {
            return null;
        } else {
            var keyClass = Class.forName(types.keyClass);
            var valClass = Class.forName(types.valClass);
            return Map.entry(keyClass, valClass);
        }
    }

    @Override
    public void registerTypesForTable(String tableName, Map.Entry<String, String> tableTypes) {
        var table = this.tableFuture.join();
        var record = new TableTypeRecord(tableTypes.getKey(), tableTypes.getValue());
        table.put(null, tableName, record);
    }

    private static class TableTypeRecord {
        private String keyClass;

        private String valClass;

        public TableTypeRecord() {
            // Intentionally left blank
        }

        public TableTypeRecord(String keyClass, String valClass) {
            this.keyClass = keyClass;
            this.valClass = valClass;
        }
    }
}
