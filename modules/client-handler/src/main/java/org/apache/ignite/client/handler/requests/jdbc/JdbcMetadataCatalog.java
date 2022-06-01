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

package org.apache.ignite.client.handler.requests.jdbc;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.apache.ignite.internal.jdbc.proto.event.JdbcPrimaryKeyMeta;
import org.apache.ignite.internal.jdbc.proto.event.JdbcTableMeta;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.manager.IgniteTables;

//TODO IGNITE-15525 Filter by table type must be added after 'view' type will appear.

/**
 * Facade over {@link IgniteTables} to get information about database entities in terms of JDBC.
 */
public class JdbcMetadataCatalog {
    /** Table name separator. */
    private static final String TABLE_NAME_SEPARATOR = "\\.";

    /** Table schema. */
    private static final int TABLE_SCHEMA = 0;

    /** Table name. */
    private static final int TABLE_NAME = 1;

    /** Primary key identifier. */
    private static final String PK = "PK_";

    /** Table type. */
    private static final String TBL_TYPE = "TABLE";

    /** Default schema name. */
    private static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    /** Ignite tables interface. Used to get all the database metadata. */
    private final IgniteTables tables;

    /** Comparator for {@link Column} by schema then table name then column order. */
    private static final Comparator<Pair<String, Column>> bySchemaThenTabNameThenColOrder
            = Comparator.comparing((Function<Pair<String, Column>, String>) Pair::getFirst)
            .thenComparingInt(o -> o.getSecond().schemaIndex());

    /** Comparator for {@link JdbcTableMeta} by table type then schema then table name. */
    private static final Comparator<Table> byTblTypeThenSchemaThenTblName = Comparator.comparing(Table::name);

    /**
     * Initializes info.
     *
     * @param tables IgniteTables.
     */
    public JdbcMetadataCatalog(IgniteTables tables) {
        this.tables = tables;
    }

    /**
     * See {@link DatabaseMetaData#getPrimaryKeys(String, String, String)} for details.
     *
     * <p>Ignite has only one possible CATALOG_NAME, it is handled on the client (driver) side.
     *
     * @param schemaNamePtrn Sql pattern for schema name.
     * @param tblNamePtrn    Sql pattern for table name.
     * @return Future of the collection of primary keys information for tables that matches specified schema and table name patterns.
     */
    public CompletableFuture<Collection<JdbcPrimaryKeyMeta>> getPrimaryKeys(String schemaNamePtrn, String tblNamePtrn) {
        String schemaNameRegex = translateSqlWildcardsToRegex(schemaNamePtrn);
        String tlbNameRegex = translateSqlWildcardsToRegex(tblNamePtrn);

        return tables.tablesAsync().thenApply(tableList -> tableList.stream()
                .filter(t -> matches(getTblSchema(t.name()), schemaNameRegex))
                .filter(t -> matches(getTblName(t.name()), tlbNameRegex))
                .map(this::createPrimaryKeyMeta)
                .collect(Collectors.toSet())
        );
    }

    /**
     * See {@link DatabaseMetaData#getTables(String, String, String, String[])} for details.
     *
     * <p>Ignite has only one possible value for CATALOG_NAME and has only one table type so these parameters are handled on the client
     * (driver) side.
     *
     * <p>Result is ordered by (schema name, table name).
     *
     * @param schemaNamePtrn Sql pattern for schema name.
     * @param tblNamePtrn    Sql pattern for table name.
     * @param tblTypes       Requested table types.
     * @return Future of the list of metadatas of tables that matches.
     */
    public CompletableFuture<List<JdbcTableMeta>> getTablesMeta(String schemaNamePtrn, String tblNamePtrn, String[] tblTypes) {
        String schemaNameRegex = translateSqlWildcardsToRegex(schemaNamePtrn);
        String tlbNameRegex = translateSqlWildcardsToRegex(tblNamePtrn);

        return tables.tablesAsync().thenApply(tablesList -> {
            return tablesList.stream()
                    .filter(t -> matches(getTblSchema(t.name()), schemaNameRegex))
                    .filter(t -> matches(getTblName(t.name()), tlbNameRegex))
                    .sorted(byTblTypeThenSchemaThenTblName)
                    .map(t -> new JdbcTableMeta(getTblSchema(t.name()), getTblName(t.name()), TBL_TYPE))
                    .collect(Collectors.toList());
        });
    }

    /**
     * See {@link DatabaseMetaData#getColumns(String, String, String, String)} for details.
     *
     * <p>Ignite has only one possible CATALOG_NAME, it is handled on the client (driver) side.
     *
     * @param schemaNamePtrn Schema name java regex pattern.
     * @param tblNamePtrn    Table name java regex pattern.
     * @param colNamePtrn    Column name java regex pattern.
     * @return Future of the list of metadatas about columns that match specified schema/tablename/columnname criterias.
     */
    public CompletableFuture<Collection<JdbcColumnMeta>> getColumnsMeta(String schemaNamePtrn, String tblNamePtrn, String colNamePtrn) {
        String schemaNameRegex = translateSqlWildcardsToRegex(schemaNamePtrn);
        String tlbNameRegex = translateSqlWildcardsToRegex(tblNamePtrn);
        String colNameRegex = translateSqlWildcardsToRegex(colNamePtrn);

        return tables.tablesAsync().thenApply(tablesList -> {
            return tablesList.stream()
                    .filter(t -> matches(getTblSchema(t.name()), schemaNameRegex))
                    .filter(t -> matches(getTblName(t.name()), tlbNameRegex))
                    .flatMap(
                        tbl -> {
                            SchemaDescriptor schema = ((TableImpl) tbl).schemaView().schema();

                            List<Pair<String, Column>> tblColPairs = new ArrayList<>();

                            for (Column column : schema.keyColumns().columns()) {
                                tblColPairs.add(new Pair<>(tbl.name(), column));
                            }

                            for (Column column : schema.valueColumns().columns()) {
                                tblColPairs.add(new Pair<>(tbl.name(), column));
                            }

                            return tblColPairs.stream();
                        })
                    .filter(e -> matches(e.getSecond().name(), colNameRegex))
                    .sorted(bySchemaThenTabNameThenColOrder)
                    .map(pair -> createColumnMeta(pair.getFirst(), pair.getSecond()))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        });
    }

    /**
     * See {@link DatabaseMetaData#getSchemas(String, String)} for details.
     *
     * <p>Ignite has only one possible CATALOG_NAME, it is handled on the client (driver) side.
     *
     * @param schemaNamePtrn Sql pattern for schema name filter.
     * @return Future of the schema names that matches provided pattern.
     */
    public CompletableFuture<Collection<String>> getSchemasMeta(String schemaNamePtrn) {
        SortedSet<String> schemas = new TreeSet<>(); // to have values sorted.

        String schemaNameRegex = translateSqlWildcardsToRegex(schemaNamePtrn);

        if (matches(DEFAULT_SCHEMA_NAME, schemaNameRegex)) {
            schemas.add(DEFAULT_SCHEMA_NAME);
        }

        return tables.tablesAsync().thenApply(tablesList ->
                tablesList.stream()
                    .map(tbl -> getTblSchema(tbl.name()))
                    .filter(schema -> matches(schema, schemaNameRegex))
                    .collect(Collectors.toCollection(() -> schemas))
        );
    }

    /**
     * Creates primary key metadata from table object.
     *
     * @param tbl Table.
     * @return Jdbc primary key metadata.
     */
    private JdbcPrimaryKeyMeta createPrimaryKeyMeta(Table tbl) {
        String schemaName = getTblSchema(tbl.name());
        String tblName = getTblName(tbl.name());

        final String keyName = PK + tblName;

        SchemaRegistry registry = ((TableImpl) tbl).schemaView();

        List<String> keyColNames = Arrays.stream(registry.schema().keyColumns().columns())
                .map(Column::name)
                .collect(Collectors.toList());

        return new JdbcPrimaryKeyMeta(schemaName, tblName, keyName, keyColNames);
    }

    /**
     * Creates column metadata from column and table name.
     *
     * @param tblName Table name.
     * @param col     Column.
     * @return Column metadata.
     */
    private JdbcColumnMeta createColumnMeta(String tblName, Column col) {
        NativeType type = col.type();

        return new JdbcColumnMeta(
                col.name(),
                getTblSchema(tblName),
                getTblName(tblName),
                col.name(),
                Commons.nativeTypeToClass(type),
                Commons.nativeTypePrecision(type),
                Commons.nativeTypeScale(type),
                col.nullable()
        );
    }

    /**
     * Splits the tableName into schema and table name and returns the table name.
     *
     * @param tblName Table name.
     * @return Table name string.
     */
    private String getTblName(String tblName) {
        return tblName.split(TABLE_NAME_SEPARATOR)[TABLE_NAME];
    }

    /**
     * Splits the tableName into schema and table name and returns the table schema.
     *
     * @param tblName Table name.
     * @return Table schema string.
     */
    private String getTblSchema(String tblName) {
        return tblName.split(TABLE_NAME_SEPARATOR)[TABLE_SCHEMA];
    }

    /**
     * Checks whether string matches SQL pattern.
     *
     * @param str     String.
     * @param sqlPtrn Pattern.
     * @return Whether string matches pattern.
     */
    private static boolean matches(String str, String sqlPtrn) {
        if (str == null) {
            return false;
        }

        if (sqlPtrn == null) {
            return true;
        }

        return str.matches(sqlPtrn);
    }

    /**
     * <p>Converts sql pattern wildcards into java regex wildcards.</p>
     * <p>Translates "_" to "." and "%" to ".*" if those are not escaped with "\" ("\_" or "\%").</p>
     * <p>All other characters are considered normal and will be escaped if necessary.</p>
     * <pre>
     * Example:
     *      som_    -->     som.
     *      so%     -->     so.*
     *      s[om]e  -->     so\[om\]e
     *      so\_me  -->     so_me
     *      some?   -->     some\?
     *      som\e   -->     som\\e
     * </pre>
     *
     * @param sqlPtrn Sql pattern.
     * @return Java regex pattern.
     */
    private static String translateSqlWildcardsToRegex(String sqlPtrn) {
        if (sqlPtrn == null || sqlPtrn.isEmpty()) {
            return sqlPtrn;
        }

        String toRegex = ' ' + sqlPtrn;

        toRegex = toRegex.replaceAll("([\\[\\]{}()*+?.\\\\\\\\^$|])", "\\\\$1");
        toRegex = toRegex.replaceAll("([^\\\\\\\\])((?:\\\\\\\\\\\\\\\\)*)%", "$1$2.*");
        toRegex = toRegex.replaceAll("([^\\\\\\\\])((?:\\\\\\\\\\\\\\\\)*)_", "$1$2.");
        toRegex = toRegex.replaceAll("([^\\\\\\\\])(\\\\\\\\(?>\\\\\\\\\\\\\\\\)*\\\\\\\\)*\\\\\\\\([_|%])", "$1$2$3");

        return toRegex.substring(1);
    }
}
