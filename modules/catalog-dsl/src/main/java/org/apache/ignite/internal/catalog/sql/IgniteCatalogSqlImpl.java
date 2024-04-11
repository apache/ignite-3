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
import org.apache.ignite.catalog.Options;
import org.apache.ignite.catalog.Query;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.sql.IgniteSql;

/**
 * Implementation of the catalog.
 */
public class IgniteCatalogSqlImpl implements IgniteCatalog {
    private final IgniteSql sql;

    private final Options options;

    public IgniteCatalogSqlImpl(IgniteSql sql, Options options) {
        this.options = options;
        this.sql = sql;
    }

    @Override
    public Query create(Class<?> keyClass, Class<?> valueClass) {
        return new CreateFromAnnotationsImpl(sql, options).processKeyValueClasses(keyClass, valueClass);
    }

    @Override
    public Query create(Class<?> recordClass) {
        return new CreateFromAnnotationsImpl(sql, options).processRecordClass(recordClass);
    }

    @Override
    public Query createTable(TableDefinition definition) {
        return new CreateFromDefinitionImpl(sql, options).from(definition);
    }

    @Override
    public Query createZone(ZoneDefinition definition) {
        return new CreateFromDefinitionImpl(sql, options).from(definition);
    }

    @Override
    public Query dropTable(TableDefinition definition) {
        return new DropTableImpl(sql, options).name(definition.schemaName(), definition.tableName()).ifExists();
    }

    @Override
    public Query dropTable(String name) {
        return new DropTableImpl(sql, options).name(name).ifExists();
    }

    @Override
    public Query dropZone(ZoneDefinition definition) {
        return new DropZoneImpl(sql, options).name(definition.zoneName()).ifExists();
    }

    @Override
    public Query dropZone(String name) {
        return new DropZoneImpl(sql, options).name(name).ifExists();
    }
}
