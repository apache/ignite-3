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

package org.apache.ignite.client.handler.requests.sql;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.client.proto.query.event.JdbcColumnMeta;
import org.apache.ignite.client.proto.query.event.JdbcPrimaryKeyMeta;
import org.apache.ignite.client.proto.query.event.JdbcTableMeta;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.manager.IgniteTables;

/**
 * Facade over {@link IgniteTables} to get information about database entities in terms of JDBC.
 */
public class JdbcMetadataInfo {
    /** Root context. Used to get all the database metadata. */
    private final IgniteTables tables;

    /** Comparator for {@link Column} by schema then table name then column order. */
    private static final Comparator<Pair<String, Column>> bySchemaThenTabNameThenColOrder
        = Comparator.comparing((Function<Pair<String, Column>, String>)Pair::getFirst)
        .thenComparingInt(o -> o.getSecond().schemaIndex());

    /** Comparator for {@link JdbcTableMeta} by table type then schema then table name. */
    private static final Comparator<Table> byTblTypeThenSchemaThenTblName = Comparator.comparing(Table::tableName);

    /**
     * Initializes info.
     *
     * @param tables IgniteTables.
     */
    public JdbcMetadataInfo(IgniteTables tables) {
        this.tables = tables;
    }

    /**
     * See {@link DatabaseMetaData#getPrimaryKeys(String, String, String)} for details.
     *
     * Ignite has only one possible CATALOG_NAME, it is handled on the client (driver) side.
     *
     * @return Collection of primary keys information for tables that matches specified schema and table name patterns.
     */
    public Collection<JdbcPrimaryKeyMeta> getPrimaryKeys(String schemaNamePtrn, String tblNamePtrn) {
        Collection<JdbcPrimaryKeyMeta> metaSet = new HashSet<>();

        String schemaNameRegex = translateSqlWildcardsToRegex(schemaNamePtrn);
        String tlbNameRegex = translateSqlWildcardsToRegex(tblNamePtrn);

        tables.tables().stream()
            .filter(t -> matches(t.tableName().split("\\.")[0], schemaNameRegex))
            .filter(t -> matches(t.tableName().split("\\.")[1], tlbNameRegex))
            .forEach(tbl -> {
                String schemaName = tbl.tableName().split("\\.")[0];
                String tblName = tbl.tableName().split("\\.")[1];

                final String keyName = "PK_" + tblName;

                SchemaRegistry registry = ((TableImpl)tbl).schemaView();

                List<String> keyColNames = Arrays.stream(registry.schema().keyColumns().columns())
                    .map(Column::name)
                    .collect(Collectors.toList());

                JdbcPrimaryKeyMeta meta = new JdbcPrimaryKeyMeta(schemaName, tblName, keyName, keyColNames);

                metaSet.add(meta);
            });

        return metaSet;
    }

    /**
     * See {@link DatabaseMetaData#getTables(String, String, String, String[])} for details.
     *
     * Ignite has only one possible value for CATALOG_NAME and has only one table type so these parameters are handled
     * on the client (driver) side.
     *
     * Result is ordered by (schema name, table name).
     *
     * @param schemaNamePtrn sql pattern for schema name.
     * @param tblNamePtrn sql pattern for table name.
     * @param tblTypes Requested table types.
     * @return List of metadatas of tables that matches.
     */
    public List<JdbcTableMeta> getTablesMeta(String schemaNamePtrn, String tblNamePtrn, String[] tblTypes) {
        String schemaNameRegex = translateSqlWildcardsToRegex(schemaNamePtrn);
        String tlbNameRegex = translateSqlWildcardsToRegex(tblNamePtrn);

        List<Table> tblsMeta = tables.tables().stream()
            .filter(t -> matches(t.tableName().split("\\.")[0], schemaNameRegex))
            .filter(t -> matches(t.tableName().split("\\.")[1], tlbNameRegex))
            .collect(Collectors.toList());

        return tblsMeta.stream()
            .sorted(byTblTypeThenSchemaThenTblName)
            .map(t -> new JdbcTableMeta(t.tableName().split("\\.")[0], t.tableName().split("\\.")[1], "TABLE"))
            .collect(Collectors.toList());
    }

    /**
     * See {@link DatabaseMetaData#getColumns(String, String, String, String)} for details.
     *
     * Ignite has only one possible CATALOG_NAME, it is handled on the client (driver) side.
     *
     * @return List of metadatas about columns that match specified schema/tablename/columnname criterias.
     */
    public Collection<JdbcColumnMeta> getColumnsMeta(String schemaNamePtrn, String tblNamePtrn, String colNamePtrn) {
        Collection<JdbcColumnMeta> metas = new LinkedHashSet<>();

        String schemaNameRegex = translateSqlWildcardsToRegex(schemaNamePtrn);
        String tlbNameRegex = translateSqlWildcardsToRegex(tblNamePtrn);
        String colNameRegex = translateSqlWildcardsToRegex(colNamePtrn);

        tables.tables().stream()
            .filter(t -> matches(t.tableName().split("\\.")[0], schemaNameRegex))
            .filter(t -> matches(t.tableName().split("\\.")[1], tlbNameRegex))
            .flatMap(
                tbl -> {
                    SchemaDescriptor schema = ((TableImpl)tbl).schemaView().schema();

                    List<Pair<String, Column>> tblColPairs = new ArrayList<>();

                    for (Column column : schema.keyColumns().columns())
                        tblColPairs.add(new Pair<>(tbl.tableName(), column));

                    for (Column column : schema.valueColumns().columns())
                        tblColPairs.add(new Pair<>(tbl.tableName(), column));

                    return tblColPairs.stream();
            })
            .filter(e -> matches(e.getSecond().name(), colNameRegex))
            .sorted(bySchemaThenTabNameThenColOrder)
            .forEachOrdered(pair -> {
                String tblName = pair.getFirst();
                Column col = pair.getSecond();

                var colMeta = new JdbcColumnMeta(
                    tblName.split("\\.")[0],
                    tblName.split("\\.")[1],
                    col.name(),
                    Commons.nativeTypeToClass(col.type()),
                    col.nullable()
                );

                if (!metas.contains(colMeta))
                    metas.add(colMeta);
            });

        return metas;
    }

    /**
     * See {@link DatabaseMetaData#getSchemas(String, String)} for details.
     *
     * Ignite has only one possible CATALOG_NAME, it is handled on the client (driver) side.
     *
     * @param schemaNamePtrn sql pattern for schema name filter.
     * @return schema names that matches provided pattern.
     */
    public Collection<String> getSchemasMeta(String schemaNamePtrn) {
        SortedSet<String> schemas = new TreeSet<>(); // to have values sorted.

        String schemaNameRegex = translateSqlWildcardsToRegex(schemaNamePtrn);

        tables.tables().stream()
            .map(e -> e.tableName().split("\\.")[0])
            .filter(e -> matches(e, schemaNameRegex))
            .forEach(schemas::add);

        return schemas;
    }

    /**
     * Checks whether string matches SQL pattern.
     *
     * @param str String.
     * @param sqlPtrn Pattern.
     * @return Whether string matches pattern.
     */
    public static boolean matches(String str, String sqlPtrn) {
        if (str == null)
            return false;

        if (sqlPtrn == null)
            return true;

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
     */
    private static String translateSqlWildcardsToRegex(String sqlPtrn) {
        if (sqlPtrn == null || sqlPtrn.isEmpty())
            return sqlPtrn;

        String toRegex = ' ' + sqlPtrn;

        toRegex = toRegex.replaceAll("([\\[\\]{}()*+?.\\\\\\\\^$|])", "\\\\$1");
        toRegex = toRegex.replaceAll("([^\\\\\\\\])((?:\\\\\\\\\\\\\\\\)*)%", "$1$2.*");
        toRegex = toRegex.replaceAll("([^\\\\\\\\])((?:\\\\\\\\\\\\\\\\)*)_", "$1$2.");
        toRegex = toRegex.replaceAll("([^\\\\\\\\])(\\\\\\\\(?>\\\\\\\\\\\\\\\\)*\\\\\\\\)*\\\\\\\\([_|%])", "$1$2$3");

        return toRegex.substring(1);
    }
}
