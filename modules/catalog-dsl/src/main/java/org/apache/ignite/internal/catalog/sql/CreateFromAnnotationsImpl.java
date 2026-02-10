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

import static org.apache.ignite.catalog.ColumnSorted.column;
import static org.apache.ignite.catalog.annotations.Table.DEFAULT_ZONE;
import static org.apache.ignite.internal.catalog.sql.QueryUtils.mapArrayToList;
import static org.apache.ignite.table.mapper.Mapper.nativelySupported;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.annotations.Column;
import org.apache.ignite.catalog.annotations.ColumnRef;
import org.apache.ignite.catalog.annotations.Id;
import org.apache.ignite.catalog.annotations.Index;
import org.apache.ignite.catalog.annotations.Table;
import org.apache.ignite.catalog.annotations.Zone;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.QualifiedName;

class CreateFromAnnotationsImpl extends AbstractCatalogQuery<TableZoneId> {
    private CreateZoneImpl createZone;

    private String zoneName;

    private CreateTableImpl createTable;

    private QualifiedName tableName;

    private IndexType pkType;

    CreateFromAnnotationsImpl(IgniteSql sql) {
        super(sql);
    }

    @Override
    protected TableZoneId result() {
        return new TableZoneId(tableName, zoneName);
    }

    CreateFromAnnotationsImpl processKeyValueClasses(Class<?> keyClass, Class<?> valueClass) {
        if (keyClass.getAnnotation(Table.class) == null && valueClass.getAnnotation(Table.class) == null) {
            throw new IllegalArgumentException(
                    "Cannot find @Table annotation neither on " + keyClass.getName() + " nor on " + valueClass.getName()
                            + ". At least one of these classes must be annotated in order to create a query object."
            );
        }

        processAnnotations(keyClass, true);
        processAnnotations(valueClass, false);
        return this;
    }

    CreateFromAnnotationsImpl processRecordClass(Class<?> recordCls) {
        if (recordCls.getAnnotation(Table.class) == null) {
            throw new IllegalArgumentException("Cannot find @Table annotation on " + recordCls.getName()
                    + ". This class must be annotated with in order to create a query object.");
        }

        processAnnotations(recordCls, true);
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

    private void processAnnotations(Class<?> clazz, boolean isKeyClass) {
        if (createTable == null) {
            createTable = new CreateTableImpl(sql).ifNotExists();
        }

        Table table = clazz.getAnnotation(Table.class);
        if (table != null) {
            String tableName = table.value().isEmpty() ? clazz.getSimpleName() : table.value();
            String schemaName = table.schemaName();
            QualifiedName qualifiedName = QualifiedName.of(schemaName, tableName);

            this.tableName = qualifiedName;
            createTable.name(qualifiedName);

            processZone(table);
            processTable(table);
        }

        processColumns(createTable, pkType, clazz, isKeyClass);
    }

    private void processZone(Table table) {
        Zone zone = table.zone();

        if (zone != null && !DEFAULT_ZONE.equalsIgnoreCase(zone.value())) {
            createZone = new CreateZoneImpl(sql).ifNotExists();

            String zoneName = zone.value();
            this.zoneName = zoneName;
            createTable.zone(zoneName);
            createZone.name(zoneName);
            createZone.storageProfiles(zone.storageProfiles());
            if (zone.partitions() > 0) {
                createZone.partitions(zone.partitions());
            }
            if (zone.replicas() > 0) {
                createZone.replicas(zone.replicas());
            }
            if (zone.quorumSize() > 0) {
                createZone.quorumSize(zone.quorumSize());
            }

            if (!zone.distributionAlgorithm().isEmpty()) {
                createZone.distributionAlgorithm(zone.distributionAlgorithm());
            }

            if (zone.dataNodesAutoAdjustScaleUp() > 0) {
                createZone.dataNodesAutoAdjustScaleUp(zone.dataNodesAutoAdjustScaleUp());
            }

            if (zone.dataNodesAutoAdjustScaleDown() > 0) {
                createZone.dataNodesAutoAdjustScaleDown(zone.dataNodesAutoAdjustScaleDown());
            }

            if (!zone.filter().isEmpty()) {
                createZone.filter(zone.filter());
            }

            if (!zone.consistencyMode().isEmpty()) {
                createZone.consistencyMode(zone.consistencyMode());
            }
        }
    }

    private void processTable(Table table) {
        for (Index ix : table.indexes()) {
            List<ColumnSorted> indexColumns = mapArrayToList(ix.columns(), col -> column(col.value(), col.sort()));
            String name = toIndexName(ix);
            createTable.addIndex(name, ix.type(), indexColumns);
        }

        ColumnRef[] colocateBy = table.colocateBy();
        if (colocateBy != null && colocateBy.length > 0) {
            createTable.colocateBy(mapArrayToList(colocateBy, ColumnRef::value));
        }

        pkType = table.primaryKeyType();
    }

    private static String toIndexName(Index ix) {
        if (!ix.value().isEmpty()) {
            return ix.value();
        }
        List<String> list = new ArrayList<>();
        list.add("ix");
        for (ColumnRef columnRef : ix.columns()) {
            list.add(columnRef.value());
        }
        return String.join("_", list);
    }

    static void processColumns(CreateTableImpl createTable, IndexType pkType, Class<?> clazz, boolean isKeyClass) {
        List<ColumnSorted> idColumns = new ArrayList<>();

        if (nativelySupported(clazz)) {
            String columnName = isKeyClass ? "id" : "val";
            if (isKeyClass) {
                idColumns.add(column(columnName));
            }
            createTable.addColumn(columnName, ColumnType.of(clazz));
        } else {
            processColumnsInPojo(createTable, clazz, idColumns);
        }

        if (!idColumns.isEmpty()) {
            createTable.primaryKey(pkType, idColumns);
        }
    }

    private static void processColumnsInPojo(CreateTableImpl createTable, Class<?> clazz, List<ColumnSorted> idColumns) {
        for (Field f : clazz.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers())) {
                continue;
            }

            String columnName;
            Column column = f.getAnnotation(Column.class);

            if (column == null) {
                columnName = f.getName();
                createTable.addColumn(columnName, ColumnType.of(f.getType()));
            } else {
                columnName = column.value().isEmpty() ? f.getName() : column.value();

                if (!column.columnDefinition().isEmpty()) {
                    createTable.addColumn(columnName, column.columnDefinition());
                } else {
                    ColumnType<?> type = ColumnType.of(f.getType(), column.length(), column.precision(), column.scale(), column.nullable());
                    createTable.addColumn(columnName, type);
                }
            }

            Id id = f.getAnnotation(Id.class);
            if (id != null) {
                idColumns.add(column(columnName, id.value()));
            }
        }
    }

}
