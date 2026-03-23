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
import static org.apache.ignite.sql.ColumnMetadata.UNDEFINED_PRECISION;
import static org.apache.ignite.sql.ColumnMetadata.UNDEFINED_SCALE;

import java.sql.DatabaseMetaData;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnContainer;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.apache.ignite.internal.jdbc.proto.event.JdbcPrimaryKeyMeta;
import org.apache.ignite.internal.jdbc.proto.event.JdbcTableMeta;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.table.IgniteTables;
import org.jetbrains.annotations.Nullable;

/**
 * Facade over {@link IgniteTables} to get information about database entities in terms of JDBC.
 */
public class JdbcMetadataCatalog {
    /** Primary key identifier. */
    private static final String PK = "PK_";

    private final ClockService clockService;

    private final SchemaSyncService schemaSyncService;

    private final CatalogService catalogService;

    /** Comparator for {@link JdbcTableMeta} by table name. */
    private static final Comparator<JdbcTableMeta> byTblTypeThenSchemaThenTblName
            = Comparator.comparing(JdbcTableMeta::tableType)
            .thenComparing(JdbcTableMeta::schemaName)
            .thenComparing(JdbcTableMeta::tableName);

    /** Comparator for {@link JdbcPrimaryKeyMeta} by table name. */
    private static final Comparator<JdbcPrimaryKeyMeta> bySchemaThenTblThenKey
            = Comparator.comparing(JdbcPrimaryKeyMeta::schemaName)
            .thenComparing(JdbcPrimaryKeyMeta::tableName)
            .thenComparing(JdbcPrimaryKeyMeta::name);

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

        return schemasAtNow().thenApply(schemas ->
                schemas.stream()
                        .filter(schema -> matches(schema.name(), schemaNameRegex))
                        .flatMap(schema -> Arrays.stream(schema.tables())
                                        .filter(table -> matches(table.name(), tlbNameRegex))
                                        .map(table -> createPrimaryKeyMeta(schema.name(), table))
                        )
                        .sorted(bySchemaThenTblThenKey)
                        .collect(toList())
        );
    }

    private CompletableFuture<Collection<CatalogSchemaDescriptor>> schemasAtNow() {
        HybridTimestamp now = clockService.now();

        return schemaSyncService.waitForMetadataCompleteness(now)
                .thenApply(unused -> catalogService.activeCatalog(now.longValue()).schemas());
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
    public CompletableFuture<List<JdbcTableMeta>> getTablesMeta(String schemaNamePtrn, String tblNamePtrn, @Nullable String[] tblTypes) {
        String schemaNameRegex = translateSqlWildcardsToRegex(schemaNamePtrn);
        String tlbNameRegex = translateSqlWildcardsToRegex(tblNamePtrn);
        Collection<CatalogObjectType> includedObjectTypes = resolveObjectTypes(tblTypes);

        return schemasAtNow().thenApply(schemas ->
                schemas.stream()
                        .filter(schema -> matches(schema.name(), schemaNameRegex))
                        .flatMap(schema -> includedObjectTypes.stream()
                                .flatMap(tblType -> getCatalogObjects(schema, tblType)
                                        .filter(desc -> matches(desc.name(), tlbNameRegex))
                                        .map(desc -> new JdbcTableMeta(schema.name(), desc.name(), tblType.name())))
                        )
                        .sorted(byTblTypeThenSchemaThenTblName)
                        .collect(toList())
        );
    }

    private static Stream<CatalogObjectDescriptor> getCatalogObjects(CatalogSchemaDescriptor schema, CatalogObjectType type) {
        switch (type) {
            case TABLE:
                return Arrays.stream(schema.tables());
            case VIEW:
                return Arrays.stream(schema.systemViews());
            default:
                throw new IllegalArgumentException("Unsupported object type: " + type);
        }
    }

    /**
     * Returns the set of table types resolved by string names.
     */
    private static Collection<CatalogObjectType> resolveObjectTypes(@Nullable String[] tblTypes) {
        if (tblTypes == null) {
            return EnumSet.allOf(CatalogObjectType.class);
        }

        Set<CatalogObjectType> types = EnumSet.noneOf(CatalogObjectType.class);

        for (String tblType : tblTypes) {
            // In ODBC, SQL_ALL_TABLE_TYPES is defined as "%", so we need to support it.
            if ("%".equals(tblType)) {
                return EnumSet.allOf(CatalogObjectType.class);
            }

            types.add(CatalogObjectType.valueOf(tblType));
        }

        return types;
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

        return schemasAtNow().thenApply(schemas -> schemas.stream()
                        .filter(schema -> matches(schema.name(), schemaNameRegex))
                        .sorted(Comparator.comparing(CatalogSchemaDescriptor::name))
                        .flatMap(schema ->
                                Stream.concat(
                                        Arrays.stream(schema.systemViews()),
                                        Arrays.stream(schema.tables())
                                )
                                .filter(desc -> matches(desc.name(), tlbNameRegex))
                                .sorted(Comparator.comparing(CatalogColumnContainer::name))
                                .flatMap(desc -> desc.columns().stream()
                                                    .filter(col -> matches(col.name(), colNameRegex))
                                                    .map(col -> createColumnMeta(schema.name(), desc.name(), col)))
                        )
                        .collect(toCollection(LinkedHashSet::new))
        );
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
        String schemaNameRegex = translateSqlWildcardsToRegex(schemaNamePtrn);

        return schemasAtNow().thenApply(schemas ->
                schemas.stream()
                        .map(CatalogObjectDescriptor::name)
                        .filter(name -> matches(name, schemaNameRegex))
                        .collect(toCollection(TreeSet::new))
        );
    }

    /**
     * Creates primary key metadata from a table descriptor.
     *
     * @param tbl Table.
     * @return Jdbc primary key metadata.
     */
    private static JdbcPrimaryKeyMeta createPrimaryKeyMeta(String schemaName, CatalogTableDescriptor tbl) {
        String keyName = PK + tbl.name();

        List<String> keyColNames = CatalogUtils.resolveColumnNames(tbl, tbl.primaryKeyColumns());

        return new JdbcPrimaryKeyMeta(schemaName, tbl.name(), keyName, keyColNames);
    }

    /**
     * Creates column metadata from column and table name.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param col Column descriptor.
     * @return Column metadata.
     */
    private static JdbcColumnMeta createColumnMeta(String schemaName, String tblName, CatalogTableColumnDescriptor col) {
        return new JdbcColumnMeta(
                col.name(),
                schemaName,
                tblName,
                col.name(),
                col.type(),
                resolvePrecision(col),
                resolveScale(col),
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
     * Converts sql pattern wildcards into java regex wildcards.
     *
     * <p>Translates "_" to "." and "%" to ".*" if those are not escaped with "\" ("\_" or "\%").</p>
     *
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

    /**
     * Resolves the precision of the specified column.
     * Returns {@link ColumnMetadata#UNDEFINED_PRECISION} if precision is not applicable for this column type.
     *
     * @return Column precision.
     */
    private static int resolvePrecision(CatalogTableColumnDescriptor column) {
        switch (column.type()) {
            case INT8:
                return 3;

            case INT16:
                return 5;

            case INT32:
                return 10;

            case INT64:
                return 19;

            case FLOAT:
            case DOUBLE:
                return 15;

            case BOOLEAN:
            case UUID:
            case DATE:
                return UNDEFINED_PRECISION;

            case DECIMAL:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
                return column.precision();

            case BYTE_ARRAY:
            case STRING:
                return column.length();

            default:
                throw new IllegalArgumentException("Unsupported type " + column.type());
        }
    }

    /**
     * Resolves the scale of the specified column.
     * Returns {@link ColumnMetadata#UNDEFINED_SCALE} if scale is not valid for this type.
     *
     * @return Number of digits of scale.
     */
    private static int resolveScale(CatalogTableColumnDescriptor column) {
        switch (column.type()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return 0;

            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case UUID:
            case DATE:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
            case BYTE_ARRAY:
            case STRING:
                return UNDEFINED_SCALE;

            case DECIMAL:
                return column.scale();

            default:
                throw new IllegalArgumentException("Unsupported type " + column.type());
        }
    }

    /** Catalog object type. */
    private enum CatalogObjectType {
        /** Name of TABLE type. */
        TABLE,

        /** Name of VIEW type. */
        VIEW
    }
}
