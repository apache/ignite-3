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

import static java.math.RoundingMode.HALF_UP;
import static java.util.Arrays.asList;
import static org.apache.ignite.configuration.schemas.table.TableIndexConfigurationSchema.HASH_INDEX_TYPE;
import static org.apache.ignite.configuration.schemas.table.TableIndexConfigurationSchema.SORTED_INDEX_TYPE;
import static org.apache.ignite.internal.util.IgniteUtils.capacity;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.ColumnChange;
import org.apache.ignite.configuration.schemas.table.ColumnTypeChange;
import org.apache.ignite.configuration.schemas.table.ColumnTypeView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultChange;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultView;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultChange;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultView;
import org.apache.ignite.configuration.schemas.table.HashIndexChange;
import org.apache.ignite.configuration.schemas.table.HashIndexView;
import org.apache.ignite.configuration.schemas.table.IndexColumnChange;
import org.apache.ignite.configuration.schemas.table.IndexColumnView;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultView;
import org.apache.ignite.configuration.schemas.table.PrimaryKeyView;
import org.apache.ignite.configuration.schemas.table.SortedIndexChange;
import org.apache.ignite.configuration.schemas.table.SortedIndexView;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexChange;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesChange;
import org.apache.ignite.internal.schema.definition.ColumnDefinitionImpl;
import org.apache.ignite.internal.schema.definition.TableDefinitionImpl;
import org.apache.ignite.internal.schema.definition.index.HashIndexDefinitionImpl;
import org.apache.ignite.internal.schema.definition.index.PrimaryKeyDefinitionImpl;
import org.apache.ignite.internal.schema.definition.index.SortedIndexColumnDefinitionImpl;
import org.apache.ignite.internal.schema.definition.index.SortedIndexDefinitionImpl;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.ColumnType.DecimalColumnType;
import org.apache.ignite.schema.definition.DefaultValueDefinition;
import org.apache.ignite.schema.definition.DefaultValueDefinition.ConstantValue;
import org.apache.ignite.schema.definition.DefaultValueDefinition.FunctionCall;
import org.apache.ignite.schema.definition.PrimaryKeyDefinition;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.schema.definition.index.HashIndexDefinition;
import org.apache.ignite.schema.definition.index.IndexColumnDefinition;
import org.apache.ignite.schema.definition.index.IndexDefinition;
import org.apache.ignite.schema.definition.index.SortOrder;
import org.apache.ignite.schema.definition.index.SortedIndexColumnDefinition;
import org.apache.ignite.schema.definition.index.SortedIndexDefinition;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration to schema and vice versa converter.
 */
public class SchemaConfigurationConverter {
    /**
     * Types map.
     */
    private static final Map<String, ColumnType> fixSizedTypes = new HashMap<>();

    static {
        putType(ColumnType.INT8);
        putType(ColumnType.INT16);
        putType(ColumnType.INT32);
        putType(ColumnType.INT64);
        putType(ColumnType.FLOAT);
        putType(ColumnType.DOUBLE);
        putType(ColumnType.UUID);
        putType(ColumnType.DATE);
    }

    /**
     * Put type.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param type Column type.
     */
    private static void putType(ColumnType type) {
        fixSizedTypes.put(type.typeSpec().name(), type);
    }

    /**
     * Convert SortedIndexColumn to IndexColumnChange.
     *
     * @param col     IndexColumnChange.
     * @param colInit IndexColumnChange to fulfill.
     * @return IndexColumnChange to get result from.
     */
    public static IndexColumnChange convert(SortedIndexColumnDefinition col, IndexColumnChange colInit) {
        colInit.changeAsc(col.sortOrder() == SortOrder.ASC);

        return colInit;
    }

    /**
     * Convert IndexColumnView to SortedIndexColumn.
     *
     * @param colCfg IndexColumnView.
     * @return SortedIndexColumn.
     */
    public static SortedIndexColumnDefinition convert(IndexColumnView colCfg) {
        return new SortedIndexColumnDefinitionImpl(colCfg.name(), colCfg.asc() ? SortOrder.ASC : SortOrder.DESC);
    }

    /**
     * Convert TableIndex to TableIndexChange.
     *
     * @param idx    TableIndex.
     * @param idxChg TableIndexChange to fulfill.
     * @return TableIndexChange to get result from.
     */
    public static TableIndexChange convert(IndexDefinition idx, TableIndexChange idxChg) {
        switch (idx.type().toUpperCase()) {
            case HASH_INDEX_TYPE:
                HashIndexDefinition hashIdx = (HashIndexDefinition) idx;

                String[] colNames = hashIdx.columns().stream().map(IndexColumnDefinition::name).toArray(String[]::new);

                return idxChg.convert(HashIndexChange.class).changeColumnNames(colNames);

            case SORTED_INDEX_TYPE:
                SortedIndexDefinition sortIdx = (SortedIndexDefinition) idx;

                return idxChg.changeUniq(sortIdx.unique())
                        .convert(SortedIndexChange.class)
                        .changeColumns(colsInit -> {
                            for (SortedIndexColumnDefinition col : sortIdx.columns()) {
                                colsInit.create(col.name(), colInit -> convert(col, colInit));
                            }
                        });

            default:
                throw new IllegalArgumentException("Unknown index type " + idx.type());
        }
    }

    /**
     * Convert TableIndexView into TableIndex.
     *
     * @param idxView TableIndexView.
     * @return TableIndex.
     */
    public static IndexDefinition convert(TableIndexView idxView) {
        String name = idxView.name();
        String type = idxView.type();

        switch (type.toUpperCase()) {
            case HASH_INDEX_TYPE:
                String[] hashCols = ((HashIndexView) idxView).columnNames();

                return new HashIndexDefinitionImpl(name, asList(hashCols));

            case SORTED_INDEX_TYPE:
                NamedListView<? extends IndexColumnView> sortedIndexColumns = ((SortedIndexView) idxView).columns();

                List<SortedIndexColumnDefinition> sortedIndexColumnDefinitions = sortedIndexColumns.namedListKeys().stream()
                        .map(sortedIndexColumns::get)
                        .map(SchemaConfigurationConverter::convert)
                        .collect(Collectors.toList());

                return new SortedIndexDefinitionImpl(name, sortedIndexColumnDefinitions, idxView.uniq());

            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    /**
     * Convert PrimaryKeyView into PrimaryKey.
     *
     * @param primaryKey PrimaryKeyView.
     * @return TableIn.
     */
    public static PrimaryKeyDefinition convert(PrimaryKeyView primaryKey) {
        return new PrimaryKeyDefinitionImpl(Set.of(primaryKey.columns()), List.of(primaryKey.colocationColumns()));
    }

    /**
     * Convert ColumnType to ColumnTypeChange.
     *
     * @param colType    ColumnType.
     * @param colTypeChg ColumnTypeChange to fulfill.
     * @return ColumnTypeChange to get result from
     */
    public static ColumnTypeChange convert(ColumnType colType, ColumnTypeChange colTypeChg) {
        String typeName = colType.typeSpec().name().toUpperCase();

        if (fixSizedTypes.containsKey(typeName)) {
            colTypeChg.changeType(typeName);
        } else {
            colTypeChg.changeType(typeName);

            switch (typeName) {
                case "BITMASK":
                case "BLOB":
                case "STRING":
                    ColumnType.VarLenColumnType varLenColType = (ColumnType.VarLenColumnType) colType;

                    colTypeChg.changeLength(varLenColType.length());

                    break;

                case "DECIMAL":
                    ColumnType.DecimalColumnType numColType = (ColumnType.DecimalColumnType) colType;

                    colTypeChg.changePrecision(numColType.precision());
                    colTypeChg.changeScale(numColType.scale());

                    break;

                case "NUMBER":
                    ColumnType.NumberColumnType numType = (ColumnType.NumberColumnType) colType;

                    colTypeChg.changePrecision(numType.precision());

                    break;

                case "TIME":
                case "DATETIME":
                case "TIMESTAMP":
                    ColumnType.TemporalColumnType temporalColType = (ColumnType.TemporalColumnType) colType;

                    colTypeChg.changePrecision(temporalColType.precision());

                    break;

                default:
                    throw new IllegalArgumentException("Unknown type " + colType.typeSpec().name());
            }
        }

        return colTypeChg;
    }

    /**
     * Convert ColumnTypeView to ColumnType.
     *
     * @param colTypeView ColumnTypeView.
     * @return ColumnType.
     */
    public static ColumnType convert(ColumnTypeView colTypeView) {
        String typeName = colTypeView.type().toUpperCase();
        ColumnType res = fixSizedTypes.get(typeName);

        if (res != null) {
            return res;
        } else {
            switch (typeName) {
                case "BITMASK":
                    int bitmaskLen = colTypeView.length();

                    return ColumnType.bitmaskOf(bitmaskLen);

                case "STRING":
                    int strLen = colTypeView.length();

                    return ColumnType.stringOf(strLen);

                case "BLOB":
                    int blobLen = colTypeView.length();

                    return ColumnType.blobOf(blobLen);

                case "DECIMAL":
                    int prec = colTypeView.precision();
                    int scale = colTypeView.scale();

                    return ColumnType.decimalOf(prec, scale);

                case "NUMBER":
                    return colTypeView.precision() == 0 ? ColumnType.number() : ColumnType.numberOf(colTypeView.precision());

                case "TIME":
                    return ColumnType.time(colTypeView.precision());

                case "DATETIME":
                    return ColumnType.datetime(colTypeView.precision());

                case "TIMESTAMP":
                    return ColumnType.timestamp(colTypeView.precision());

                default:
                    throw new IllegalArgumentException("Unknown type " + typeName);
            }
        }
    }

    /**
     * Convert column to column change.
     *
     * @param col    Column to convert.
     * @param colChg Column
     * @return ColumnChange to get result from.
     */
    public static ColumnChange convert(ColumnDefinition col, ColumnChange colChg) {
        colChg.changeType(colTypeInit -> convert(col.type(), colTypeInit));

        if (col.defaultValueDefinition() != null) {
            colChg.changeDefaultValueProvider(colDefault -> {
                switch (col.defaultValueDefinition().type()) {
                    case CONSTANT:
                        ConstantValue constantValue = col.defaultValueDefinition();

                        colDefault.convert(ConstantValueDefaultChange.class).changeDefaultValue(
                                convertDefaultToConfiguration(constantValue.value(), col.type())
                        );

                        break;
                    case FUNCTION_CALL:
                        FunctionCall functionCall = col.defaultValueDefinition();

                        colDefault.convert(FunctionCallDefaultChange.class).changeFunctionName(functionCall.functionName());

                        break;
                    case NULL:
                        // do nothing
                        break;
                    default:
                        throw new IllegalStateException("Unknown default value definition type [type="
                                + col.defaultValueDefinition().type() + ']');
                }
            });
        }

        colChg.changeNullable(col.nullable());

        return colChg;
    }

    /**
     * Convert column view to Column.
     *
     * @param colView Column view.
     * @return Column.
     */
    public static ColumnDefinition convert(ColumnView colView) {
        var type = convert(colView.type());

        DefaultValueDefinition valueSupplier;

        var defaultValueProvider = colView.defaultValueProvider();

        if (defaultValueProvider instanceof NullValueDefaultView) {
            valueSupplier = DefaultValueDefinition.nullValue();
        } else if (defaultValueProvider instanceof FunctionCallDefaultView) {
            valueSupplier = DefaultValueDefinition.functionCall(((FunctionCallDefaultView) defaultValueProvider).functionName());
        } else if (defaultValueProvider instanceof ConstantValueDefaultView) {
            valueSupplier = DefaultValueDefinition.constant(
                    convertDefaultFromConfiguration(((ConstantValueDefaultView) defaultValueProvider).defaultValue(), type)
            );
        } else {
            throw new IllegalStateException("Unknown value supplier class " + defaultValueProvider.getClass().getName());
        }

        return new ColumnDefinitionImpl(
                colView.name(),
                type,
                colView.nullable(),
                valueSupplier
        );
    }

    /**
     * Convert table schema to table changer.
     *
     * @param tbl    Table schema to convert.
     * @param tblChg Change to fulfill.
     * @return TableChange to get result from.
     */
    public static TableChange convert(TableDefinition tbl, TableChange tblChg) {
        tblChg.changeIndices(idxsChg -> {
            for (IndexDefinition idx : tbl.indices()) {
                idxsChg.create(idx.name(), idxInit -> convert(idx, idxInit));
            }
        });

        tblChg.changeColumns(colsChg -> {
            for (ColumnDefinition col : tbl.columns()) {
                colsChg.create(col.name(), colChg -> convert(col, colChg));
            }
        });

        tblChg.changePrimaryKey(pkCng -> pkCng.changeColumns(tbl.keyColumns().toArray(String[]::new))
                .changeColocationColumns(tbl.colocationColumns().toArray(String[]::new)));

        return tblChg;
    }

    /**
     * Convert TableConfiguration to TableSchema.
     *
     * @param tblCfg TableConfiguration to convert.
     * @return Table schema.
     */
    public static TableDefinition convert(TableConfiguration tblCfg) {
        return convert(tblCfg.value());
    }

    /**
     * Convert table configuration view to table schema.
     *
     * @param tblView TableView to convert.
     * @return Table schema.
     */
    public static TableDefinitionImpl convert(TableView tblView) {
        String canonicalName = tblView.name();
        int sepPos = canonicalName.indexOf('.');
        String schemaName = canonicalName.substring(0, sepPos);
        String tableName = canonicalName.substring(sepPos + 1);

        NamedListView<? extends ColumnView> colsView = tblView.columns();

        var columns = new LinkedHashMap<String, ColumnDefinition>(capacity(colsView.size()));

        for (String key : colsView.namedListKeys()) {
            ColumnView colView = colsView.get(key);

            if (colView != null) {
                ColumnDefinition definition = convert(colView);

                columns.put(definition.name(), definition);
            }
        }

        NamedListView<? extends TableIndexView> idxsView = tblView.indices();

        var indices = new HashMap<String, IndexDefinition>(capacity(idxsView.size()));

        for (String key : idxsView.namedListKeys()) {
            TableIndexView indexView = idxsView.get(key);

            if (indexView == null) { // skip just deleted indices
                continue;
            }

            IndexDefinition definition = convert(indexView);

            indices.put(definition.name(), definition);
        }

        PrimaryKeyDefinition primaryKey = convert(tblView.primaryKey());

        return new TableDefinitionImpl(schemaName, tableName, columns, primaryKey, indices);
    }

    /**
     * Create table.
     *
     * @param tbl        Table to create.
     * @param tblsChange Tables change to fulfill.
     * @return TablesChange to get result from.
     */
    public static TablesChange createTable(TableDefinition tbl, TablesChange tblsChange) {
        return tblsChange.changeTables(tblsChg -> tblsChg.create(tbl.canonicalName(), tblChg -> convert(tbl, tblChg)));
    }

    /**
     * Drop table.
     *
     * @param tbl        table to drop.
     * @param tblsChange TablesChange change to fulfill.
     * @return TablesChange to get result from.
     */
    public static TablesChange dropTable(TableDefinition tbl, TablesChange tblsChange) {
        return tblsChange.changeTables(schmTblChange -> schmTblChange.delete(tbl.canonicalName()));
    }

    /**
     * Add index.
     *
     * @param idx       Index to add.
     * @param tblChange TableChange to fulfill.
     * @return TableChange to get result from.
     */
    public static TableChange addIndex(IndexDefinition idx, TableChange tblChange) {
        return tblChange.changeIndices(idxsChg -> idxsChg.create(idx.name(), idxChg -> convert(idx, idxChg)));
    }

    /**
     * Drop index.
     *
     * @param indexName Index name to drop.
     * @param tblChange Table change to fulfill.
     * @return TableChange to get result from.
     */
    public static TableChange dropIndex(String indexName, TableChange tblChange) {
        return tblChange.changeIndices(idxChg -> idxChg.delete(indexName));
    }

    /**
     * Add table column.
     *
     * @param column    Column to add.
     * @param tblChange TableChange to fulfill.
     * @return TableChange to get result from.
     */
    public static TableChange addColumn(ColumnDefinition column, TableChange tblChange) {
        return tblChange.changeColumns(colsChg -> colsChg.create(column.name(), colChg -> convert(column, colChg)));
    }

    /**
     * Drop table column.
     *
     * @param columnName column name to drop.
     * @param tblChange  TableChange to fulfill.
     * @return TableChange to get result from.
     */
    public static TableChange dropColumn(String columnName, TableChange tblChange) {
        return tblChange.changeColumns(colChg -> colChg.delete(columnName));
    }

    /**
     * Converts literal to a string representation.
     *
     * @param defaultValue Value to convert.
     * @param type Type iof the value.
     * @return String representation of given literal.
     * @throws NullPointerException If given value or type is null.
     */
    public static String convertDefaultToConfiguration(Object defaultValue, ColumnType type) {
        switch (type.typeSpec()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case DATE:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
            case NUMBER:
            case STRING:
            case UUID:
                return defaultValue.toString();
            case BLOB:
                return IgniteUtils.toHexString((byte[]) defaultValue);
            case BITMASK:
                return IgniteUtils.toHexString(((BitSet) defaultValue).toByteArray());
            default:
                throw new IllegalStateException("Unknown type [type=" + type + ']');
        }
    }

    private static @Nullable Object convertDefaultFromConfiguration(String defaultValue, ColumnType type) {
        switch (type.typeSpec()) {
            case INT8:
                return Byte.parseByte(defaultValue);
            case INT16:
                return Short.parseShort(defaultValue);
            case INT32:
                return Integer.parseInt(defaultValue);
            case INT64:
                return Long.parseLong(defaultValue);
            case FLOAT:
                return Float.parseFloat(defaultValue);
            case DOUBLE:
                return Double.parseDouble(defaultValue);
            case DECIMAL:
                assert type instanceof DecimalColumnType;

                return new BigDecimal(defaultValue).setScale(((DecimalColumnType) type).scale(), HALF_UP);
            case DATE:
                return LocalDate.parse(defaultValue);
            case TIME:
                return LocalTime.parse(defaultValue);
            case DATETIME:
                return LocalDateTime.parse(defaultValue);
            case TIMESTAMP:
                return Instant.parse(defaultValue);
            case NUMBER:
                return new BigInteger(defaultValue);
            case STRING:
                return defaultValue;
            case UUID:
                return UUID.fromString(defaultValue);
            case BLOB:
                return IgniteUtils.fromHexString(defaultValue);
            case BITMASK:
                return BitSet.valueOf(IgniteUtils.fromHexString(defaultValue));
            default:
                throw new IllegalStateException("Unknown type [type=" + type + ']');
        }
    }
}
