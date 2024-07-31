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

import static org.apache.ignite.internal.catalog.sql.CreateFromAnnotationsImpl.processColumns;
import static org.apache.ignite.internal.catalog.sql.QueryUtils.isGreaterThanZero;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.apache.ignite.catalog.definitions.IndexDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.sql.IgniteSql;

class CreateFromDefinitionImpl extends AbstractCatalogQuery<TableZoneId> {
    private CreateZoneImpl createZone;

    private String zoneName;

    private CreateTableImpl createTable;

    private String tableName;

    CreateFromDefinitionImpl(IgniteSql sql) {
        super(sql);
    }

    @Override
    protected TableZoneId result() {
        return new TableZoneId(tableName, zoneName);
    }

    CreateFromDefinitionImpl from(ZoneDefinition def) {
        createZone = new CreateZoneImpl(sql);
        String zoneName = def.zoneName();
        this.zoneName = zoneName;
        createZone.name(zoneName);
        createZone.storageProfiles(def.storageProfiles());
        if (def.ifNotExists()) {
            createZone.ifNotExists();
        }
        if (isGreaterThanZero(def.partitions())) {
            createZone.partitions(def.partitions());
        }
        if (isGreaterThanZero(def.replicas())) {
            createZone.replicas(def.replicas());
        }

        if (!StringUtils.nullOrBlank(def.affinityFunction())) {
            createZone.affinity(def.affinityFunction());
        }

        if (isGreaterThanZero(def.dataNodesAutoAdjust())) {
            createZone.dataNodesAutoAdjust(def.dataNodesAutoAdjust());
        }
        if (isGreaterThanZero(def.dataNodesAutoAdjustScaleUp())) {
            createZone.dataNodesAutoAdjustScaleUp(def.dataNodesAutoAdjustScaleUp());
        }
        if (isGreaterThanZero(def.dataNodesAutoAdjustScaleDown())) {
            createZone.dataNodesAutoAdjustScaleDown(def.dataNodesAutoAdjustScaleDown());
        }

        if (!StringUtils.nullOrBlank(def.filter())) {
            createZone.filter(def.filter());
        }

        return this;
    }

    CreateFromDefinitionImpl from(TableDefinition def) {
        createTable = new CreateTableImpl(sql);
        String tableName = def.tableName();
        this.tableName = tableName;
        createTable.name(def.schemaName(), tableName);
        if (def.ifNotExists()) {
            createTable.ifNotExists();
        }
        if (!nullOrEmpty(def.colocationColumns())) {
            createTable.colocateBy(def.colocationColumns());
        }
        if (def.zoneName() != null) {
            createTable.zone(def.zoneName());
        }

        IndexType pkType = def.primaryKeyType() == null ? IndexType.DEFAULT : def.primaryKeyType();
        if (def.keyClass() != null) {
            processColumns(createTable, pkType, def.keyClass(), true);
            if (def.valueClass() != null) {
                processColumns(createTable, pkType, def.valueClass(), false);
            }
        } else {
            List<ColumnDefinition> columns = def.columns();
            if (columns != null) {
                for (ColumnDefinition column : columns) {
                    if (column.type() != null) {
                        createTable.addColumn(column.name(), column.type());
                    } else if (column.definition() != null) {
                        createTable.addColumn(column.name(), column.definition());
                    }
                }
            }
            if (!nullOrEmpty(def.primaryKeyColumns())) {
                createTable.primaryKey(pkType, def.primaryKeyColumns());
            }
        }

        List<IndexDefinition> indexes = def.indexes();
        if (indexes != null) {
            for (IndexDefinition ix : indexes) {
                createTable.addIndex(toIndexName(ix), ix.type(), ix.columns());
            }
        }
        return this;
    }

    @Override
    protected void accept(QueryContext ctx) {
        if (createZone != null) {
            ctx.visit(createZone).formatSeparator();
        }
        if (createTable != null) {
            ctx.visit(createTable).formatSeparator();
        }
    }

    private static String toIndexName(IndexDefinition ix) {
        if (!StringUtils.nullOrEmpty(ix.name())) {
            return ix.name();
        }
        List<String> list = new ArrayList<>();
        list.add("ix");
        for (ColumnSorted col : ix.columns()) {
            list.add(col.columnName());
        }
        return String.join("_", list);
    }

    private static boolean nullOrEmpty(Collection<?> c) {
        return c == null || c.isEmpty();
    }
}
