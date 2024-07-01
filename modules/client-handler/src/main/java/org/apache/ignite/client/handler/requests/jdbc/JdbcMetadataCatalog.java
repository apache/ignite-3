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

package org.apache.ignite.client.handler.requests.jdbc;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.sql.DatabaseMetaData;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.apache.ignite.internal.jdbc.proto.event.JdbcPrimaryKeyMeta;
import org.apache.ignite.internal.jdbc.proto.event.JdbcTableMeta;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.catalog.CatalogToSchemaDescriptorConverter;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.table.IgniteTables;
import org.jetbrains.annotations.Nullable;

// TODO IGNITE-15525 Filter by table type must be added after 'view' type will appear.

/**
 * Facade over {@link IgniteTables} to get information about database entities in terms of JDBC.
 */
public class JdbcMetadataCatalog {
    /** Primary key identifier. */
    private static final String PK = "PK_";

    /** Table type. */
    private static final String TBL_TYPE = "TABLE";

    private final ClockService clockService;

    private final SchemaSyncService schemaSyncService;

    private final CatalogService catalogService;

    /** Comparator for {@link Column} by schema then table name then column order. */
    private static final Comparator<Pair<String, Column>> bySchemaThenTabNameThenColOrder
            = Comparator.comparing((Function<Pair<String, Column>, String>) Pair::getFirst)
            .thenComparingInt(o -> o.getSecond().positionInRow());

    /** Comparator for {@link JdbcTableMeta} by table name. */
    private static final Comparator<CatalogTableDescriptor> byTblTypeThenSchemaThenTblName
            = Comparator.comparing(CatalogTableDescriptor::name);

    /**
     * Initializes info.
     *
     * @param clockService Clock service.
     * @param schemaSyncService Used to wait for schemas' completeness.
     * @param catalogService Used to get table descriptions.
     */
    public JdbcMetadataCatalog(ClockService clockService, SchemaSyncService schemaSyncService, CatalogService catalogService) {
        this.clockService = clockService;
        this.schemaSyncService = schemaSyncService;
        this.catalogService = catalogService;
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

        return tablesAtNow()
                .thenApply(tables -> tables.stream()
                        .filter(t -> tableNameAndSchemaMatches(t, schemaNameRegex, tlbNameRegex))
                        .map(this::createPrimaryKeyMeta)
                        .collect(toSet())
                );
    }

    private CompletableFuture<Collection<CatalogTableDescriptor>> tablesAtNow() {
        HybridTimestamp now = clockService.now();

        return schemaSyncService.waitForMetadataCompleteness(now)
                .thenApply(unused -> catalogService.tables(catalogService.activeCatalogVersion(now.longValue())));
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

        return tablesAtNow().thenApply(tables -> {
            return tables.stream()
                    .filter(t -> tableNameAndSchemaMatches(t, schemaNameRegex, tlbNameRegex))
                    .sorted(byTblTypeThenSchemaThenTblName)
                    .map(t -> new JdbcTableMeta(SqlCommon.DEFAULT_SCHEMA_NAME, t.name(), TBL_TYPE))
                    .collect(toList());
        });
    }

    private static boolean tableNameAndSchemaMatches(
            CatalogTableDescriptor table,
            @Nullable String schemaNameRegex,
            @Nullable String tlbNameRegex
    ) {
        return matches(SqlCommon.DEFAULT_SCHEMA_NAME, schemaNameRegex) && matches(table.name(), tlbNameRegex);
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

        return tablesAtNow().thenApply(tablesList -> tablesList.stream()
                .filter(t -> tableNameAndSchemaMatches(t, schemaNameRegex, tlbNameRegex))
                .flatMap(
                    tbl -> {
                        SchemaDescriptor schema = CatalogToSchemaDescriptorConverter.convert(tbl, tbl.tableVersion());

                        return schema.columns().stream()
                                .map(column -> new Pair<>(tbl.name(), column));
                    })
                .filter(e -> matches(e.getSecond().name(), colNameRegex))
                .sorted(bySchemaThenTabNameThenColOrder)
                .map(pair -> createColumnMeta(pair.getFirst(), pair.getSecond()))
                .collect(toCollection(LinkedHashSet::new)));
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

        if (matches(SqlCommon.DEFAULT_SCHEMA_NAME, schemaNameRegex)) {
            schemas.add(SqlCommon.DEFAULT_SCHEMA_NAME);
        }

        return tablesAtNow().thenApply(tables ->
                tables.stream()
                    .map(tbl -> SqlCommon.DEFAULT_SCHEMA_NAME)
                    .filter(schema -> matches(schema, schemaNameRegex))
                    .collect(toCollection(() -> schemas))
        );
    }

    /**
     * Creates primary key metadata from a table descriptor.
     *
     * @param tbl Table.
     * @return Jdbc primary key metadata.
     */
    private JdbcPrimaryKeyMeta createPrimaryKeyMeta(CatalogTableDescriptor tbl) {
        String keyName = PK + tbl.name();

        List<String> keyColNames = List.copyOf(tbl.primaryKeyColumns());

        return new JdbcPrimaryKeyMeta(SqlCommon.DEFAULT_SCHEMA_NAME, tbl.name(), keyName, keyColNames);
    }

    /**
     * Creates column metadata from column and table name.
     *
     * @param tblName Table name.
     * @param col     Column.
     * @return Column metadata.
     */
    private JdbcColumnMeta createColumnMeta(String tblName, Column col) {
        NativeType colType = col.type();

        return new JdbcColumnMeta(
                col.name(),
                SqlCommon.DEFAULT_SCHEMA_NAME,
                tblName,
                col.name(),
                colType.spec().asColumnType(), 
                Commons.nativeTypePrecision(colType),
                Commons.nativeTypeScale(colType),
                col.nullable()
        );
    }

    /**
     * Checks whether string matches SQL pattern.
     *
     * @param str     String.
     * @param sqlPtrn Pattern.
     * @return Whether string matches pattern.
     */
    private static boolean matches(@Nullable String str, @Nullable String sqlPtrn) {
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
    private static @Nullable String translateSqlWildcardsToRegex(String sqlPtrn) {
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
