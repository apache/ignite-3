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

package org.apache.ignite.internal.schema.configuration;

import org.apache.ignite.configuration.schemas.table.ColumnChange;
import org.apache.ignite.configuration.schemas.table.ColumnTypeChange;
import org.apache.ignite.configuration.schemas.table.ColumnTypeView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.IndexColumnChange;
import org.apache.ignite.configuration.schemas.table.IndexColumnView;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexChange;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesChange;
import org.apache.ignite.configuration.tree.NamedListView;
import org.apache.ignite.internal.schema.ColumnImpl;
import org.apache.ignite.internal.schema.HashIndexImpl;
import org.apache.ignite.internal.schema.PartialIndexImpl;
import org.apache.ignite.internal.schema.PrimaryIndexImpl;
import org.apache.ignite.internal.schema.SchemaTableImpl;
import org.apache.ignite.internal.schema.SortedIndexColumnImpl;
import org.apache.ignite.internal.schema.SortedIndexImpl;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.HashIndex;
import org.apache.ignite.schema.IndexColumn;
import org.apache.ignite.schema.PartialIndex;
import org.apache.ignite.schema.PrimaryIndex;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.SortedIndex;
import org.apache.ignite.schema.SortedIndexColumn;
import org.apache.ignite.schema.TableIndex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Configuration to schema and vice versa converter. */
public class SchemaConfigurationConverter {
    /** Hash index type. */
    private static final String HASH_TYPE = "HASH";

    /** Sorted index type. */
    private static final String SORTED_TYPE = "SORTED";

    /** Partial index type. */
    private static final String PARTIAL_TYPE = "PARTIAL";

    /** Primary key index type. */
    private static final String PK_TYPE = "PK";

    /** Types map. */
    private static Map<String, ColumnType> types = new HashMap<>();

    static {
        putType(ColumnType.INT8);
        putType(ColumnType.INT16);
        putType(ColumnType.INT32);
        putType(ColumnType.INT64);
        putType(ColumnType.UINT8);
        putType(ColumnType.UINT16);
        putType(ColumnType.UINT32);
        putType(ColumnType.UINT64);
        putType(ColumnType.FLOAT);
        putType(ColumnType.DOUBLE);
        putType(ColumnType.UUID);
        putType(ColumnType.string());
        putType(ColumnType.blobOf());
        // TODO; handle length for some types
        //putType(ColumnType.bitmaskOf());
    }

    private static void putType(ColumnType type) {
        types.put(type.typeSpec().name(), type);
    }

    /**
     * Convert SortedIndexColumn to IndexColumnChange.
     * @param col IndexColumnChange.
     * @param colInit IndexColumnChange to fullfill.
     */
    public static void convert(SortedIndexColumn col, IndexColumnChange colInit) {
        colInit.changeName(col.name());
        colInit.changeAsc(col.asc());
    }

    /**
     * Convert IndexColumnView to SortedIndexColumn.
     *
     * @param colCfg IndexColumnView.
     * @return SortedIndexColumn.
     */
    public static SortedIndexColumn convert(IndexColumnView colCfg) {
        return new SortedIndexColumnImpl(colCfg.name(), colCfg.asc());
    }

    /**
     * Convert TableIndex to TableIndexChange.
     *
     * @param idx TableIndex.
     * @param idxChg TableIndexChange to fullfill.
     */
    public static void convert(TableIndex idx, TableIndexChange idxChg) {
        idxChg.changeName(idx.name());
        idxChg.changeType(idx.type());

        if (HASH_TYPE.equals(idx.type())) {
            HashIndex hashIdx = (HashIndex)idx;

            String[] colNames = hashIdx.columns().stream().map(IndexColumn::name).toArray(String[]::new);
            idxChg.changeColNames(colNames);
        }
        else if (PARTIAL_TYPE.equals(idx.type())) {
            PartialIndex partIdx = (PartialIndex)idx;

            idxChg.changeUniq(partIdx.unique());
            idxChg.changeExpr(partIdx.expr());

            idxChg.changeColumns(colsChg -> {
                for (SortedIndexColumn col : partIdx.columns())
                    colsChg.create(col.name(), colInit -> convert(col, colInit));
            });

        }
        else if (SORTED_TYPE.equals(idx.type())) {
            SortedIndex sortIdx = (SortedIndex)idx;
            idxChg.changeUniq(sortIdx.unique());

            idxChg.changeColumns(colsInit -> {
                for (SortedIndexColumn col : sortIdx.columns())
                    colsInit.create(col.name(), colInit -> convert(col, colInit));
            });
        }
        else if (PK_TYPE.equals(idx.type())) {
            PrimaryIndex primIdx = (PrimaryIndex)idx;

            idxChg.changeColumns(colsInit -> {
                for (SortedIndexColumn col : primIdx.columns())
                    colsInit.create(col.name(), colInit -> convert(col, colInit));
            });

            idxChg.changeAffinityColumns(primIdx.affinityColumns().toArray(new String[primIdx.affinityColumns().size()]));
        }
        else throw new IllegalArgumentException("Unknown index type " + idx.type());
    }

    /**
     * Convert TableIndexView into TableIndex.
     *
     * @param idxView TableIndexView.
     * @return TableIndex.
     */
    public static TableIndex convert(TableIndexView idxView) {
        String name = idxView.name();
        String type = idxView.type();

        if (type.equals("HASH")) {
            String[] cols = idxView.colNames();

            return new HashIndexImpl(name, cols);
        }
        else if (type.equals("SORTED") || type.equals("PARTIAL") || type.equals("PK")) {
            if (type.equals("PARTIAL")) {
                boolean uniq = idxView.uniq();
                String expr = idxView.expr();
                NamedListView<? extends IndexColumnView> colsView = idxView.columns();
                List<SortedIndexColumn> cols = new ArrayList<>();
                for (String key : idxView.columns().namedListKeys()) {
                    SortedIndexColumn col = convert(colsView.get(key));
                    cols.add(col);
                }

                return new PartialIndexImpl(name, cols, uniq, expr);
            }
            if (type.equals("SORTED")) {
                boolean uniq = idxView.uniq();
                List<SortedIndexColumn> cols = new ArrayList<>();
                for (String key : idxView.columns().namedListKeys()) {
                    SortedIndexColumn col = convert(idxView.columns().get(key));
                    cols.add(col);
                }

                return new SortedIndexImpl(name, cols, uniq);
            }
            if (type.equals("PK")) {
                List<SortedIndexColumn> cols = new ArrayList<>();
                for (String key : idxView.columns().namedListKeys()) {
                    SortedIndexColumn col = convert(idxView.columns().get(key));
                    cols.add(col);
                }
                String[] affCols = idxView.colNames();

                return new PrimaryIndexImpl(cols, Arrays.stream(affCols).collect(Collectors.toList()));
            }

            return null;
        }
        else
            throw new IllegalArgumentException("Unknown type " + type);
    }

    /**
     * Convert ColumnType to ColumnTypeChange.
     *
     * @param colType ColumnType.
     * @param colTypeChg ColumnTypeChange to fullfill.
     */
    public static void convert(ColumnType colType, ColumnTypeChange colTypeChg) {
        colTypeChg.changeType(colType.typeSpec().name());
        // TODO varlen types
    }

    /**
     * Convert ColumnTypeView to ColumnType.
     *
     * @param colTypeView ColumnTypeView.
     * @return ColumnType.
     */
    public static ColumnType convert(ColumnTypeView colTypeView) {
        String typeName = colTypeView.type();
        ColumnType res = types.get(typeName);
        if (res != null)
            return res;
        else
            // TODO varlen types
            throw new IllegalArgumentException();
    }

    /**
     * Convert column to column change.
     *
     * @param col Column to convert.
     * @param colChg Column
     */
    public static void convert(Column col, ColumnChange colChg) {
        colChg.changeName(col.name());
        colChg.changeType(colTypeInit -> convert(col.type(), colTypeInit));
        if (col.defaultValue() != null)
            colChg.changeDefaultValue(col.defaultValue().toString()); // TODO: specify "default type" type
        colChg.changeNullable(col.nullable());
    }

    /**
     * Convert column view to Column.
     *
     * @param colView Column view.
     * @return Column.
     */
    public static Column convert(ColumnView colView) {
        String name = colView.name();
        ColumnType type = convert(colView.type());
        boolean nullable = colView.nullable();
        String defValue = colView.defaultValue();

        return new ColumnImpl(name, type, nullable, defValue);
    }

    /**
     * Convert schema table to schema table change.
     *
     * @param tbl Schema table to convert.
     * @param tblChg Change to fullfill.
     */
    public static void convert(SchemaTable tbl, TableChange tblChg) {
        tblChg.changeName(tbl.canonicalName());
        tblChg.changeIndices(idxsChg -> {
            for (TableIndex idx : tbl.indices())
                idxsChg.create(idx.name(), idxInit -> convert(idx, idxInit));
        });
        tblChg.changeColumns(colsChg -> {
            for (Column col : tbl.keyColumns())
                colsChg.create(col.name(), colChg -> convert(col, colChg));
            for (Column col : tbl.valueColumns())
                colsChg.create(col.name(), colChg -> convert(col, colChg));
        });
    }

    public static SchemaTable convert(TableConfiguration tblCfg) {
        return convert(tblCfg.value());
    }

    /**
     * Convert configuration to SchemaTable.
     *
     * @param tblView SchmTableView to convert.
     * @return SchemaTable.
     */
    public static SchemaTableImpl convert(TableView tblView) {
        String canonicalName = tblView.name();

        NamedListView<? extends ColumnView> colsView = tblView.columns();
        LinkedHashMap<String, Column> columns = new LinkedHashMap<>(colsView.size());
        for (String key : colsView.namedListKeys()) {
            ColumnView colView = colsView.get(key);
            Column col = convert(colView);
            columns.put(col.name(), col);
        }

        NamedListView<? extends TableIndexView> idxsView = tblView.indices();
        Map<String, TableIndex> indices = new HashMap<>(idxsView.size());
        for (String key : idxsView.namedListKeys()) {
            TableIndexView idxView = idxsView.get(key);
            TableIndex idx = convert(idxView);
            indices.put(idx.name(), idx);
        }

        // TUDO: schema name
        return new SchemaTableImpl("PUBLIC", canonicalName, columns, indices);
    }

    /**
     * Create table.
     *
     * @param tbl Table to create.
     * @param tblsChange Tables change to fulfill.
     * @return Schema change to get result from.
     */
    public static TablesChange createTable(SchemaTable tbl, TablesChange tblsChange) {
        return tblsChange.changeTables(tblsChg -> tblsChg.create(tbl.canonicalName(), tblChg -> convert(tbl, tblChg)));
    }

    /**
     * Drop table.
     *
     * @param tbl table to drop.
     * @param tblsChange Schanmge change to fulfill.
     * @return Schema change to get result from.
     */
    public static TablesChange dropTable(SchemaTable tbl, TablesChange tblsChange) {
        return tblsChange.changeTables(schmTblChange -> schmTblChange.delete(tbl.canonicalName()));
    }

    /**
     * Add index.
     *
     * @param idx Index to add.
     * @param tblChange TableChange to fullfill.
     * @return TableChange to get result from.
     */
    public static TableChange addIndex(TableIndex idx, TableChange tblChange) {
        return tblChange.changeIndices(idxsChg -> idxsChg.create(idx.name(), idxChg -> convert(idx, idxChg)));
    }

    /**
     * Drop index.
     *
     * @param indexName Index name to drop.
     * @param schmTblChange Table change to fulfill.
     * @return Schema table change to get result from.
     */
    public static TableChange dropIndex(String indexName, TableChange schmTblChange) {
        return schmTblChange.changeIndices(idxChg -> idxChg.delete(indexName));
    }

    /**
     * Add table column.
     *
     * @param column Column to add.
     * @param schmTblChange Schamge table change to fulfill.
     * @return Schema table change to get result from.
     */
    public static TableChange addColumn(Column column, TableChange schmTblChange) {
        return schmTblChange.changeColumns(colsChg -> colsChg.create(column.name(), colChg -> convert(column, colChg)));
    }

    /**
     * Drop table column.
     *
     * @param columnName column name to drop.
     * @param schmTblChange Schamge table change to fulfill.
     * @return Schema table change to get result from.
     */
    public static TableChange dropColumn(String columnName, TableChange schmTblChange) {
        return schmTblChange.changeColumns(colChg -> colChg.delete(columnName));
    }
}
