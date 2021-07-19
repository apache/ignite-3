/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client.fakes;

import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.manager.IgniteTables;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class FakeIgniteTables implements IgniteTables, IgniteTablesInternal {
    public static final String TABLE_EXISTS = "Table exists";

    private final ConcurrentHashMap<String, TableImpl> tables = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<UUID, TableImpl> tablesById = new ConcurrentHashMap<>();

    @Override public Table createTable(String name, Consumer<TableChange> tableInitChange) {
        var newTable = getNewTable(name);

        var oldTable = tables.putIfAbsent(name, newTable);

        if (oldTable != null)
            throw new IgniteException(TABLE_EXISTS);

        tablesById.put(newTable.tableId(), newTable);

        return newTable;
    }

    @Override public void alterTable(String name, Consumer<TableChange> tableChange) {
        throw new IgniteException("Not supported");
    }

    @Override public Table getOrCreateTable(String name, Consumer<TableChange> tableInitChange) {
        var newTable = getNewTable(name);

        var oldTable = tables.putIfAbsent(name, newTable);

        if (oldTable != null)
            return oldTable;

        tablesById.put(newTable.tableId(), newTable);

        return newTable;
    }

    @Override public void dropTable(String name) {
        var table = tables.remove(name);

        if (table != null)
            tablesById.remove(table.tableId());
    }

    @Override public List<Table> tables() {
        return new ArrayList<>(tables.values());
    }

    @Override public Table table(String name) {
        return tables.get(name);
    }

    @NotNull private TableImpl getNewTable(String name) {
        return new TableImpl(new FakeInternalTable(name, UUID.randomUUID()), new SchemaRegistryImpl(v -> null), null, null);
    }

    @Override public TableImpl table(UUID id, boolean checkConfiguration) {
        return tablesById.get(id);
    }
}
