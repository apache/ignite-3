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

package org.apache.ignite.internal.catalog.sql;

import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.Table;

/**
 * Implementation of the catalog.
 */
public class IgniteCatalogSqlImpl implements IgniteCatalog {
    private final IgniteSql sql;

    private final IgniteTables tables;

    public IgniteCatalogSqlImpl(IgniteSql sql, IgniteTables tables) {
        this.sql = sql;
        this.tables = tables;
    }

    @Override
    public Table create(Class<?> keyClass, Class<?> valueClass) {
        TableZoneId tableZoneId = new CreateFromAnnotationsImpl(sql).processKeyValueClasses(keyClass, valueClass).execute();
        return tables.table(tableZoneId.tableName());
    }

    @Override
    public Table create(Class<?> recordClass) {
        TableZoneId tableZoneId = new CreateFromAnnotationsImpl(sql).processRecordClass(recordClass).execute();
        return tables.table(tableZoneId.tableName());
    }

    @Override
    public Table createTable(TableDefinition definition) {
        TableZoneId tableZoneId = new CreateFromDefinitionImpl(sql).from(definition).execute();
        return tables.table(tableZoneId.tableName());
    }

    @Override
    public void createZone(ZoneDefinition definition) {
        new CreateFromDefinitionImpl(sql).from(definition);
    }

    @Override
    public void dropTable(TableDefinition definition) {
        new DropTableImpl(sql).name(definition.schemaName(), definition.tableName()).ifExists();
    }

    @Override
    public void dropTable(String name) {
        new DropTableImpl(sql).name(name).ifExists();
    }

    @Override
    public void dropZone(ZoneDefinition definition) {
        new DropZoneImpl(sql).name(definition.zoneName()).ifExists();
    }

    @Override
    public void dropZone(String name) {
        new DropZoneImpl(sql).name(name).ifExists();
    }
}
