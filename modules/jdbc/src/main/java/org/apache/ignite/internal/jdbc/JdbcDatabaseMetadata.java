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

package org.apache.ignite.internal.jdbc;

import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.RowIdLifetime.ROWID_UNSUPPORTED;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.jdbc.JdbcDatabaseMetadataUtils.createObjectListResultSet;
import static org.apache.ignite.internal.jdbc.proto.SqlStateCode.CONNECTION_CLOSED;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.jdbc.proto.IgniteQueryErrorCode;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryEventHandler;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaPrimaryKeysRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaPrimaryKeysResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaSchemasRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaSchemasResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcPrimaryKeyMeta;
import org.apache.ignite.internal.jdbc.proto.event.JdbcTableMeta;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.sql.ColumnMetadataImpl;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;

/**
 * JDBC database metadata implementation.
 */
public class JdbcDatabaseMetadata implements DatabaseMetaData {
    /** Driver name. */
    public static final String DRIVER_NAME = "Apache Ignite JDBC Driver";

    /** Product name. */
    public static final String PRODUCT_NAME = "Apache Ignite";

    /** The only possible name for catalog. */
    public static final String CATALOG_NAME = "IGNITE";

    /** Name of TABLE type. */
    public static final String TYPE_TABLE = "TABLE";

    /** Name of system view type. */
    public static final String TYPE_VIEW = "VIEW";

    private final JdbcQueryEventHandler handler;

    private final String url;

    private final String userName;

    private final Connection connection;

    /**
     * Constructor.
     *
     * @param connection Connection
     * @param handler Handler.
     * @param url URL
     * @param userName User name,
     */
    public JdbcDatabaseMetadata(
            Connection connection,
            JdbcQueryEventHandler handler,
            String url,
            String userName
    ) {
        this.handler = handler;
        this.connection = connection;
        this.url = url;
        this.userName = userName;
    }

    /** {@inheritDoc} */
    @Override
    public boolean allProceduresAreCallable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public String getURL() {
        return url;
    }

    /** {@inheritDoc} */
    @Override
    public String getUserName() {
        return userName;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isReadOnly() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nullsAreSortedHigh() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nullsAreSortedLow() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nullsAreSortedAtStart() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nullsAreSortedAtEnd() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public String getDatabaseProductName() {
        return PRODUCT_NAME;
    }

    /** {@inheritDoc} */
    @Override
    public String getDatabaseProductVersion() {
        return ProtocolVersion.LATEST_VER.toString();
    }

    /** {@inheritDoc} */
    @Override
    public String getDriverName() {
        return DRIVER_NAME;
    }

    /** {@inheritDoc} */
    @Override
    public String getDriverVersion() {
        return IgniteProductVersion.CURRENT_VERSION.toString();
    }

    /** {@inheritDoc} */
    @Override
    public int getDriverMajorVersion() {
        return IgniteProductVersion.CURRENT_VERSION.major();
    }

    /** {@inheritDoc} */
    @Override
    public int getDriverMinorVersion() {
        return IgniteProductVersion.CURRENT_VERSION.minor();
    }

    /** {@inheritDoc} */
    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean storesUpperCaseIdentifiers() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean storesMixedCaseIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public String getIdentifierQuoteString() {
        return "\"";
    }

    /** {@inheritDoc} */
    @Override
    public String getSQLKeywords() {
        return "";
    }

    /** {@inheritDoc} */
    @Override
    public String getNumericFunctions() {
        return "";
    }

    /** {@inheritDoc} */
    @Override
    public String getStringFunctions() {
        return "";
    }

    /** {@inheritDoc} */
    @Override
    public String getSystemFunctions() {
        return "";
    }

    /** {@inheritDoc} */
    @Override
    public String getTimeDateFunctions() {
        return "";
    }

    /** {@inheritDoc} */
    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    /** {@inheritDoc} */
    @Override
    public String getExtraNameCharacters() {
        return "";
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsColumnAliasing() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nullPlusNonNullIsNull() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsConvert() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsConvert(int fromType, int toType) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsTableCorrelationNames() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsOrderByUnrelated() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsGroupByUnrelated() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsLikeEscapeClause() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsMultipleResultSets() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsMultipleTransactions() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsNonNullableColumns() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsMinimumSQLGrammar() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsCoreSQLGrammar() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsExtendedSQLGrammar() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        // TODO IGNITE-15527
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsOuterJoins() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsFullOuterJoins() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsLimitedOuterJoins() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public String getSchemaTerm() {
        return "schema";
    }

    /** {@inheritDoc} */
    @Override
    public String getProcedureTerm() {
        return "";
    }

    /** {@inheritDoc} */
    @Override
    public String getCatalogTerm() {
        return "";
    }

    /** {@inheritDoc} */
    @Override
    public boolean isCatalogAtStart() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public String getCatalogSeparator() {
        return "";
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsSchemasInDataManipulation() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsSubqueriesInComparisons() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsSubqueriesInExists() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsSubqueriesInIns() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsUnion() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxBinaryLiteralLength() {
        return Integer.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxCharLiteralLength() {
        return Integer.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxColumnNameLength() {
        return 128;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxColumnsInIndex() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxColumnsInSelect() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxColumnsInTable() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxConnections() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxIndexLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxSchemaNameLength() {
        return 128;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxCatalogNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxRowSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxStatementLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxStatements() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxTableNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxTablesInSelect() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getDefaultTransactionIsolation() {
        return TRANSACTION_SERIALIZABLE;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsTransactions() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return level == TRANSACTION_SERIALIZABLE;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getProcedures(String catalog, String schemaPtrn,
            String procedureNamePtrn) throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(asList(
                columnMeta("PROCEDURE_CAT", ColumnType.STRING),
                columnMeta("PROCEDURE_SCHEM", ColumnType.STRING),
                columnMeta("PROCEDURE_NAME", ColumnType.STRING),
                columnMeta("", ColumnType.NULL),
                columnMeta("", ColumnType.NULL),
                columnMeta("", ColumnType.NULL),
                columnMeta("REMARKS", ColumnType.STRING),
                columnMeta("PROCEDURE_TYPE", ColumnType.INT16),
                columnMeta("SPECIFIC_NAME", ColumnType.STRING)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPtrn, String procedureNamePtrn,
            String colNamePtrn) throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(asList(
                columnMeta("PROCEDURE_CAT", ColumnType.STRING),
                columnMeta("PROCEDURE_SCHEM", ColumnType.STRING),
                columnMeta("PROCEDURE_NAME", ColumnType.STRING),
                columnMeta("COLUMN_NAME", ColumnType.STRING),
                columnMeta("COLUMN_TYPE", ColumnType.INT16),
                columnMeta("DATA_TYPE", ColumnType.INT32),
                columnMeta("TYPE_NAME", ColumnType.STRING),
                columnMeta("PRECISION", ColumnType.INT32),
                columnMeta("LENGTH", ColumnType.INT32),
                columnMeta("SCALE", ColumnType.INT16),
                columnMeta("RADIX", ColumnType.INT16),
                columnMeta("NULLABLE", ColumnType.INT16),
                columnMeta("REMARKS", ColumnType.STRING),
                columnMeta("COLUMN_DEF", ColumnType.STRING),
                columnMeta("SQL_DATA_TYPE", ColumnType.INT32),
                columnMeta("SQL_DATETIME_SUB", ColumnType.INT32),
                columnMeta("CHAR_OCTET_LENGTH", ColumnType.INT32),
                columnMeta("ORDINAL_POSITION", ColumnType.INT32),
                columnMeta("IS_NULLABLE", ColumnType.STRING),
                columnMeta("SPECIFIC_NAME", ColumnType.STRING)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getTables(String catalog, String schemaPtrn, String tblNamePtrn, String[] tblTypes)
            throws SQLException {
        ensureNotClosed();

        List<ColumnMetadata> meta = asList(
                columnMeta("TABLE_CAT", ColumnType.STRING),
                columnMeta("TABLE_SCHEM", ColumnType.STRING),
                columnMeta("TABLE_NAME", ColumnType.STRING),
                columnMeta("TABLE_TYPE", ColumnType.STRING),
                columnMeta("REMARKS", ColumnType.STRING),
                columnMeta("TYPE_CAT", ColumnType.STRING),
                columnMeta("TYPE_SCHEM", ColumnType.STRING),
                columnMeta("TYPE_NAME", ColumnType.STRING),
                columnMeta("SELF_REFERENCING_COL_NAME", ColumnType.STRING),
                columnMeta("REF_GENERATION", ColumnType.STRING));

        boolean tblTypeMatch = false;

        if (tblTypes == null) {
            tblTypeMatch = true;
        } else {
            for (String type : tblTypes) {
                if (TYPE_TABLE.equals(type) || TYPE_VIEW.equals(type)) {
                    tblTypeMatch = true;

                    break;
                }
            }
        }

        if (!isValidCatalog(catalog) || !tblTypeMatch) {
            return createObjectListResultSet(meta);
        }

        try {
            JdbcMetaTablesResult res
                    = handler.tablesMetaAsync(new JdbcMetaTablesRequest(schemaPtrn, tblNamePtrn, tblTypes)).get();

            if (!res.success()) {
                throw IgniteQueryErrorCode.createJdbcSqlException(res.err(), res.status());
            }

            List<List<Object>> rows = new LinkedList<>();

            for (JdbcTableMeta tblMeta : res.meta()) {
                rows.add(tableRow(tblMeta));
            }

            return createObjectListResultSet(rows, meta);
        } catch (InterruptedException e) {
            throw new SQLException("Thread was interrupted.", e);
        } catch (ExecutionException e) {
            throw new SQLException("Metadata request failed.", e);
        } catch (CancellationException e) {
            throw new SQLException("Metadata request canceled.", SqlStateCode.QUERY_CANCELLED);
        }
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, "%");
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getSchemas(String catalog, String schemaPtrn) throws SQLException {
        ensureNotClosed();

        List<ColumnMetadata> meta = asList(
                columnMeta("TABLE_SCHEM", ColumnType.STRING),
                columnMeta("TABLE_CATALOG", ColumnType.STRING)
        );

        if (!isValidCatalog(catalog)) {
            return createObjectListResultSet(meta);
        }

        try {
            JdbcMetaSchemasResult res = handler.schemasMetaAsync(new JdbcMetaSchemasRequest(schemaPtrn)).get();

            if (!res.success()) {
                throw IgniteQueryErrorCode.createJdbcSqlException(res.err(), res.status());
            }

            List<List<Object>> rows = new LinkedList<>();

            for (String schema : res.schemas()) {
                List<Object> row = new ArrayList<>(2);

                row.add(schema);
                row.add(CATALOG_NAME);

                rows.add(row);
            }

            return createObjectListResultSet(rows, meta);
        } catch (InterruptedException e) {
            throw new SQLException("Thread was interrupted.", e);
        } catch (ExecutionException e) {
            throw new SQLException("Metadata request failed.", e);
        } catch (CancellationException e) {
            throw new SQLException("Metadata request canceled.", e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    @Override
    public ResultSet getCatalogs() throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(singletonList(singletonList(CATALOG_NAME)),
                asList(columnMeta("TABLE_CAT", ColumnType.STRING)));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    @Override
    public ResultSet getTableTypes() throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(
                asList(List.of(TYPE_TABLE, TYPE_VIEW)),
                asList(columnMeta("TABLE_TYPE", ColumnType.STRING)));
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getColumns(String catalog, String schemaPtrn, String tblNamePtrn, String colNamePtrn) throws SQLException {
        ensureNotClosed();

        List<ColumnMetadata> meta = asList(
                columnMeta("TABLE_CAT", ColumnType.STRING),      // 1
                columnMeta("TABLE_SCHEM", ColumnType.STRING),    // 2
                columnMeta("TABLE_NAME", ColumnType.STRING),     // 3
                columnMeta("COLUMN_NAME", ColumnType.STRING),    // 4
                columnMeta("DATA_TYPE", ColumnType.INT32),       // 5
                columnMeta("TYPE_NAME", ColumnType.STRING),      // 6
                columnMeta("COLUMN_SIZE", ColumnType.INT32),   // 7
                columnMeta("BUFFER_LENGTH", ColumnType.INT32), // 8
                columnMeta("DECIMAL_DIGITS", ColumnType.INT32), // 9
                columnMeta("NUM_PREC_RADIX", ColumnType.INT16),  // 10
                columnMeta("NULLABLE", ColumnType.INT32),        // 11
                columnMeta("REMARKS", ColumnType.STRING),        // 12
                columnMeta("COLUMN_DEF", ColumnType.STRING),     // 13
                columnMeta("SQL_DATA_TYPE", ColumnType.INT32), // 14
                columnMeta("SQL_DATETIME_SUB", ColumnType.INT32), // 15
                columnMeta("CHAR_OCTET_LENGTH", ColumnType.INT32), // 16
                columnMeta("ORDINAL_POSITION", ColumnType.INT32), // 17
                columnMeta("IS_NULLABLE", ColumnType.STRING),    // 18
                columnMeta("SCOPE_CATLOG", ColumnType.STRING),   // 19
                columnMeta("SCOPE_SCHEMA", ColumnType.STRING),   // 20
                columnMeta("SCOPE_TABLE", ColumnType.STRING),    // 21
                columnMeta("SOURCE_DATA_TYPE", ColumnType.INT16), // 22
                columnMeta("IS_AUTOINCREMENT", ColumnType.STRING), // 23
                columnMeta("IS_GENERATEDCOLUMN", ColumnType.STRING) // 24
        );

        if (!isValidCatalog(catalog)) {
            return createObjectListResultSet(meta);
        }

        try {
            JdbcMetaColumnsResult res = handler.columnsMetaAsync(new JdbcMetaColumnsRequest(schemaPtrn, tblNamePtrn, colNamePtrn))
                    .get();

            if (!res.success()) {
                throw IgniteQueryErrorCode.createJdbcSqlException(res.err(), res.status());
            }

            List<List<Object>> rows = new LinkedList<>();

            for (int i = 0; i < res.meta().size(); ++i) {
                rows.add(columnRow(res.meta().get(i), i + 1));
            }

            return createObjectListResultSet(rows, meta);
        } catch (InterruptedException e) {
            throw new SQLException("Thread was interrupted.", e);
        } catch (ExecutionException e) {
            throw new SQLException("Metadata request failed.", e);
        } catch (CancellationException e) {
            throw new SQLException("Metadata request canceled.", SqlStateCode.QUERY_CANCELLED);
        }
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String tbl,
            String colNamePtrn) throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(asList(
                columnMeta("TABLE_CAT", ColumnType.STRING),
                columnMeta("TABLE_SCHEM", ColumnType.STRING),
                columnMeta("TABLE_NAME", ColumnType.STRING),
                columnMeta("COLUMN_NAME", ColumnType.STRING),
                columnMeta("GRANTOR", ColumnType.STRING),
                columnMeta("GRANTEE", ColumnType.STRING),
                columnMeta("PRIVILEGE", ColumnType.STRING),
                columnMeta("IS_GRANTABLE", ColumnType.STRING)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPtrn,
            String tblNamePtrn) throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(asList(
                columnMeta("TABLE_CAT", ColumnType.STRING),
                columnMeta("TABLE_SCHEM", ColumnType.STRING),
                columnMeta("TABLE_NAME", ColumnType.STRING),
                columnMeta("GRANTOR", ColumnType.STRING),
                columnMeta("GRANTEE", ColumnType.STRING),
                columnMeta("PRIVILEGE", ColumnType.STRING),
                columnMeta("IS_GRANTABLE", ColumnType.STRING)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String tbl, int scope,
            boolean nullable) throws SQLException {
        return createObjectListResultSet(asList(
                columnMeta("SCOPE", ColumnType.INT16),
                columnMeta("COLUMN_NAME", ColumnType.STRING),
                columnMeta("DATA_TYPE", ColumnType.INT32),
                columnMeta("TYPE_NAME", ColumnType.STRING),
                columnMeta("COLUMN_SIZE", ColumnType.INT32),
                columnMeta("BUFFER_LENGTH", ColumnType.INT32),
                columnMeta("DECIMAL_DIGITS", ColumnType.INT16),
                columnMeta("PSEUDO_COLUMN", ColumnType.INT16)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String tbl) throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(asList(
                columnMeta("SCOPE", ColumnType.INT16),
                columnMeta("COLUMN_NAME", ColumnType.STRING),
                columnMeta("DATA_TYPE", ColumnType.INT32),
                columnMeta("TYPE_NAME", ColumnType.STRING),
                columnMeta("COLUMN_SIZE", ColumnType.INT32),
                columnMeta("BUFFER_LENGTH", ColumnType.INT32),
                columnMeta("DECIMAL_DIGITS", ColumnType.INT16),
                columnMeta("PSEUDO_COLUMN", ColumnType.INT16)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String tbl) throws SQLException {
        ensureNotClosed();

        List<ColumnMetadata> meta = asList(
                columnMeta("TABLE_CAT", ColumnType.STRING),
                columnMeta("TABLE_SCHEM", ColumnType.STRING),
                columnMeta("TABLE_NAME", ColumnType.STRING),
                columnMeta("COLUMN_NAME", ColumnType.STRING),
                columnMeta("KEY_SEQ", ColumnType.INT16),
                columnMeta("PK_NAME", ColumnType.STRING));

        if (!isValidCatalog(catalog)) {
            return createObjectListResultSet(meta);
        }

        try {
            JdbcMetaPrimaryKeysResult res = handler.primaryKeysMetaAsync(new JdbcMetaPrimaryKeysRequest(schema, tbl)).get();

            if (!res.success()) {
                throw IgniteQueryErrorCode.createJdbcSqlException(res.err(), res.status());
            }

            List<List<Object>> rows = new LinkedList<>();

            for (JdbcPrimaryKeyMeta pkMeta : res.meta()) {
                rows.addAll(primaryKeyRows(pkMeta));
            }

            return createObjectListResultSet(rows, meta);
        } catch (InterruptedException e) {
            throw new SQLException("Thread was interrupted.", e);
        } catch (ExecutionException e) {
            throw new SQLException("Metadata request failed.", e);
        } catch (CancellationException e) {
            throw new SQLException("Metadata request canceled.", SqlStateCode.QUERY_CANCELLED);
        }
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String tbl) throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(asList(
                columnMeta("PKTABLE_CAT", ColumnType.STRING),
                columnMeta("PKTABLE_SCHEM", ColumnType.STRING),
                columnMeta("PKTABLE_NAME", ColumnType.STRING),
                columnMeta("PKCOLUMN_NAME", ColumnType.STRING),
                columnMeta("FKTABLE_CAT", ColumnType.STRING),
                columnMeta("FKTABLE_SCHEM", ColumnType.STRING),
                columnMeta("FKTABLE_NAME", ColumnType.STRING),
                columnMeta("FKCOLUMN_NAME", ColumnType.STRING),
                columnMeta("KEY_SEQ", ColumnType.INT16),
                columnMeta("UPDATE_RULE", ColumnType.INT16),
                columnMeta("DELETE_RULE", ColumnType.INT16),
                columnMeta("FK_NAME", ColumnType.STRING),
                columnMeta("PK_NAME", ColumnType.STRING),
                columnMeta("DEFERRABILITY", ColumnType.INT16)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String tbl) throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(asList(
                columnMeta("PKTABLE_CAT", ColumnType.STRING),
                columnMeta("PKTABLE_SCHEM", ColumnType.STRING),
                columnMeta("PKTABLE_NAME", ColumnType.STRING),
                columnMeta("PKCOLUMN_NAME", ColumnType.STRING),
                columnMeta("FKTABLE_CAT", ColumnType.STRING),
                columnMeta("FKTABLE_SCHEM", ColumnType.STRING),
                columnMeta("FKTABLE_NAME", ColumnType.STRING),
                columnMeta("FKCOLUMN_NAME", ColumnType.STRING),
                columnMeta("KEY_SEQ", ColumnType.INT16),
                columnMeta("UPDATE_RULE", ColumnType.INT16),
                columnMeta("DELETE_RULE", ColumnType.INT16),
                columnMeta("FK_NAME", ColumnType.STRING),
                columnMeta("PK_NAME", ColumnType.STRING),
                columnMeta("DEFERRABILITY", ColumnType.INT16)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTbl,
            String foreignCatalog, String foreignSchema, String foreignTbl) throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(asList(
                columnMeta("PKTABLE_CAT", ColumnType.STRING),
                columnMeta("PKTABLE_SCHEM", ColumnType.STRING),
                columnMeta("PKTABLE_NAME", ColumnType.STRING),
                columnMeta("PKCOLUMN_NAME", ColumnType.STRING),
                columnMeta("FKTABLE_CAT", ColumnType.STRING),
                columnMeta("FKTABLE_SCHEM", ColumnType.STRING),
                columnMeta("FKTABLE_NAME", ColumnType.STRING),
                columnMeta("FKCOLUMN_NAME", ColumnType.STRING),
                columnMeta("KEY_SEQ", ColumnType.INT16),
                columnMeta("UPDATE_RULE", ColumnType.INT16),
                columnMeta("DELETE_RULE", ColumnType.INT16),
                columnMeta("FK_NAME", ColumnType.STRING),
                columnMeta("PK_NAME", ColumnType.STRING),
                columnMeta("DEFERRABILITY", ColumnType.INT16)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getTypeInfo() throws SQLException {
        ensureNotClosed();

        List<List<Object>> types = new ArrayList<>(21);

        types.add(asList("BOOLEAN", Types.BOOLEAN, 1, null, null, null,
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "BOOLEAN", 0, 0,
                Types.BOOLEAN, 0, 10));

        types.add(asList("TINYINT", Types.TINYINT, 3, null, null, null,
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "TINYINT", 0, 0,
                Types.TINYINT, 0, 10));

        types.add(asList("SMALLINT", Types.SMALLINT, 5, null, null, null,
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "SMALLINT", 0, 0,
                Types.SMALLINT, 0, 10));

        types.add(asList("INTEGER", Types.INTEGER, 10, null, null, null,
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "INTEGER", 0, 0,
                Types.INTEGER, 0, 10));

        types.add(asList("BIGINT", Types.BIGINT, 19, null, null, null,
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "BIGINT", 0, 0,
                Types.BIGINT, 0, 10));

        types.add(asList("FLOAT", Types.FLOAT, 17, null, null, null,
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "FLOAT", 0, 0,
                Types.FLOAT, 0, 10));

        types.add(asList("REAL", Types.REAL, 7, null, null, null,
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "REAL", 0, 0,
                Types.REAL, 0, 10));

        types.add(asList("DOUBLE", Types.DOUBLE, 17, null, null, null,
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "DOUBLE", 0, 0,
                Types.DOUBLE, 0, 10));

        types.add(asList("NUMERIC", Types.NUMERIC, Integer.MAX_VALUE, null, null, "PRECISION,SCALE",
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "NUMERIC", 0, 0,
                Types.NUMERIC, 0, 10));

        types.add(asList("DECIMAL", Types.DECIMAL, Integer.MAX_VALUE, null, null, "PRECISION,SCALE",
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "DECIMAL", 0, 0,
                Types.DECIMAL, 0, 10));

        types.add(asList("DATE", Types.DATE, 8, "DATE '", "'", null,
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "DATE", 0, 0,
                Types.DATE, 0, null));

        types.add(asList("TIME", Types.TIME, 6, "TIME '", "'", null,
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "TIME", 0, 0,
                Types.TIME, 0, null));

        types.add(asList("TIMESTAMP", Types.TIMESTAMP, 23, "TIMESTAMP '", "'", null,
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "TIMESTAMP", 0, 10,
                Types.TIMESTAMP, 0, null));

        types.add(asList("CHAR", Types.CHAR, Integer.MAX_VALUE, "'", "'", "LENGTH",
                (short) typeNullable, true, (short) typeSearchable, false, false, false, "CHAR", 0, 0,
                Types.CHAR, 0, null));

        types.add(asList("VARCHAR", Types.VARCHAR, Integer.MAX_VALUE, "'", "'", "LENGTH",
                (short) typeNullable, true, (short) typeSearchable, false, false, false, "VARCHAR", 0, 0,
                Types.VARCHAR, 0, null));

        types.add(asList("LONGVARCHAR", Types.LONGVARCHAR, Integer.MAX_VALUE, "'", "'", "LENGTH",
                (short) typeNullable, true, (short) typeSearchable, false, false, false, "LONGVARCHAR", 0, 0,
                Types.LONGVARCHAR, 0, null));

        types.add(asList("BINARY", Types.BINARY, Integer.MAX_VALUE, "'", "'", "LENGTH",
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "BINARY", 0, 0,
                Types.BINARY, 0, null));

        types.add(asList("VARBINARY", Types.VARBINARY, Integer.MAX_VALUE, "'", "'", "LENGTH",
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "VARBINARY", 0, 0,
                Types.VARBINARY, 0, null));

        types.add(asList("LONGVARBINARY", Types.LONGVARBINARY, Integer.MAX_VALUE, "'", "'", "LENGTH",
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "LONGVARBINARY", 0, 0,
                Types.LONGVARBINARY, 0, null));

        types.add(asList("UUID", Types.OTHER, ColumnMetadata.UNDEFINED_PRECISION, "'", "'", null,
                (short) typeNullable, false, (short) typeSearchable, true, false, false, null, 0, 0,
                Types.OTHER, null, 10));

        // IgniteCustomType: JDBC catalog level information for your type.

        types.add(asList("OTHER", Types.OTHER, Integer.MAX_VALUE, "'", "'", "LENGTH",
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "OTHER", 0, 0,
                Types.OTHER, 0, null));

        types.add(asList("ARRAY", Types.ARRAY, 0, "(", "')", null,
                (short) typeNullable, false, (short) typeSearchable, false, false, false, "ARRAY", 0, 0,
                Types.ARRAY, 0, null));

        return createObjectListResultSet(types, asList(
                columnMeta("TYPE_NAME", ColumnType.STRING),
                columnMeta("DATA_TYPE", ColumnType.INT32),
                columnMeta("PRECISION", ColumnType.INT32),
                columnMeta("LITERAL_PREFIX", ColumnType.STRING),
                columnMeta("LITERAL_SUFFIX", ColumnType.STRING),
                columnMeta("CREATE_PARAMS", ColumnType.STRING),
                columnMeta("NULLABLE", ColumnType.INT16),
                columnMeta("CASE_SENSITIVE", ColumnType.BOOLEAN),
                columnMeta("SEARCHABLE", ColumnType.INT16),
                columnMeta("UNSIGNED_ATTRIBUTE", ColumnType.BOOLEAN),
                columnMeta("FIXED_PREC_SCALE", ColumnType.BOOLEAN),
                columnMeta("AUTO_INCREMENT", ColumnType.BOOLEAN),
                columnMeta("LOCAL_TYPE_NAME", ColumnType.STRING),
                columnMeta("MINIMUM_SCALE", ColumnType.INT16),
                columnMeta("MAXIMUM_SCALE", ColumnType.INT16),
                columnMeta("SQL_DATA_TYPE", ColumnType.INT32),
                columnMeta("SQL_DATETIME_SUB", ColumnType.INT32),
                columnMeta("NUM_PREC_RADIX", ColumnType.INT32)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String tbl, boolean unique,
            boolean approximate) throws SQLException {
        ensureNotClosed();

        List<ColumnMetadata> meta = asList(
                columnMeta("TABLE_CAT", ColumnType.STRING),
                columnMeta("TABLE_SCHEM", ColumnType.STRING),
                columnMeta("TABLE_NAME", ColumnType.STRING),
                columnMeta("NON_UNIQUE", ColumnType.BOOLEAN),
                columnMeta("INDEX_QUALIFIER", ColumnType.STRING),
                columnMeta("INDEX_NAME", ColumnType.STRING),
                columnMeta("TYPE", ColumnType.INT16),
                columnMeta("ORDINAL_POSITION", ColumnType.INT16),
                columnMeta("COLUMN_NAME", ColumnType.STRING),
                columnMeta("ASC_OR_DESC", ColumnType.STRING),
                columnMeta("CARDINALITY", ColumnType.INT32),
                columnMeta("PAGES", ColumnType.INT32),
                columnMeta("FILTER_CONDITION", ColumnType.STRING));

        if (!isValidCatalog(catalog)) {
            return createObjectListResultSet(meta);
        }

        throw new UnsupportedOperationException("Index info is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsResultSetType(int type) {
        return type == TYPE_FORWARD_ONLY;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return supportsResultSetType(type) && concurrency == CONCUR_READ_ONLY;
    }

    /** {@inheritDoc} */
    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean updatesAreDetected(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean insertsAreDetected(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsBatchUpdates() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getUDTs(String catalog, String schemaPtrn, String typeNamePtrn,
            int[] types) throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(asList(
                columnMeta("TYPE_CAT", ColumnType.STRING),
                columnMeta("TYPE_SCHEM", ColumnType.STRING),
                columnMeta("TYPE_NAME", ColumnType.STRING),
                columnMeta("CLASS_NAME", ColumnType.STRING),
                columnMeta("DATA_TYPE", ColumnType.INT32),
                columnMeta("REMARKS", ColumnType.STRING),
                columnMeta("BASE_TYPE", ColumnType.INT16)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public Connection getConnection() {
        return connection;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsNamedParameters() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPtrn,
            String typeNamePtrn) throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(asList(
                columnMeta("TYPE_CAT", ColumnType.STRING),
                columnMeta("TYPE_SCHEM", ColumnType.STRING),
                columnMeta("TYPE_NAME", ColumnType.STRING),
                columnMeta("SUPERTYPE_CAT", ColumnType.STRING),
                columnMeta("SUPERTYPE_SCHEM", ColumnType.STRING),
                columnMeta("SUPERTYPE_NAME", ColumnType.STRING)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getSuperTables(String catalog, String schemaPtrn,
            String tblNamePtrn) throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(asList(
                columnMeta("TABLE_CAT", ColumnType.STRING),
                columnMeta("TABLE_SCHEM", ColumnType.STRING),
                columnMeta("TABLE_NAME", ColumnType.STRING),
                columnMeta("SUPERTABLE_NAME", ColumnType.STRING)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getAttributes(String catalog, String schemaPtrn, String typeNamePtrn,
            String attributeNamePtrn) throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(asList(
                columnMeta("TYPE_CAT", ColumnType.STRING),
                columnMeta("TYPE_SCHEM", ColumnType.STRING),
                columnMeta("TYPE_NAME", ColumnType.STRING),
                columnMeta("ATTR_NAME", ColumnType.STRING),
                columnMeta("DATA_TYPE", ColumnType.INT32),
                columnMeta("ATTR_TYPE_NAME", ColumnType.STRING),
                columnMeta("ATTR_SIZE", ColumnType.INT32),
                columnMeta("DECIMAL_DIGITS", ColumnType.INT32),
                columnMeta("NUM_PREC_RADIX", ColumnType.INT32),
                columnMeta("NULLABLE", ColumnType.INT32),
                columnMeta("REMARKS", ColumnType.STRING),
                columnMeta("ATTR_DEF", ColumnType.STRING),
                columnMeta("SQL_DATA_TYPE", ColumnType.INT32),
                columnMeta("SQL_DATETIME_SUB", ColumnType.INT32),
                columnMeta("CHAR_OCTET_LENGTH", ColumnType.INT32),
                columnMeta("ORDINAL_POSITION", ColumnType.INT32),
                columnMeta("IS_NULLABLE", ColumnType.STRING),
                columnMeta("SCOPE_CATALOG", ColumnType.STRING),
                columnMeta("SCOPE_SCHEMA", ColumnType.STRING),
                columnMeta("SCOPE_TABLE", ColumnType.STRING),
                columnMeta("SOURCE_DATA_TYPE", ColumnType.INT16)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return holdability == CLOSE_CURSORS_AT_COMMIT;
    }

    /** {@inheritDoc} */
    @Override
    public int getResultSetHoldability() {
        return CLOSE_CURSORS_AT_COMMIT;
    }

    /** {@inheritDoc} */
    @Override
    public int getDatabaseMajorVersion() {
        return ProtocolVersion.LATEST_VER.major();
    }

    /** {@inheritDoc} */
    @Override
    public int getDatabaseMinorVersion() {
        return ProtocolVersion.LATEST_VER.minor();
    }

    /** {@inheritDoc} */
    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override
    public int getJDBCMinorVersion() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getSQLStateType() {
        return sqlStateSQL99;
    }

    /** {@inheritDoc} */
    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public RowIdLifetime getRowIdLifetime() {
        return ROWID_UNSUPPORTED;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        // We do not check whether connection is closed as 
        // this operation is not expected to do any server calls.  
        return createObjectListResultSet(asList(
                columnMeta("NAME", ColumnType.STRING),
                columnMeta("MAX_LEN", ColumnType.INT32),
                columnMeta("DEFAULT_VALUE", ColumnType.STRING),
                columnMeta("DESCRIPTION", ColumnType.STRING)
        ));
    }

    // TODO IGNITE-15529 List all supported functions

    /** {@inheritDoc} */
    @Override
    public ResultSet getFunctions(String catalog, String schemaPtrn,
            String functionNamePtrn) throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(asList(
                columnMeta("FUNCTION_CAT", ColumnType.STRING),
                columnMeta("FUNCTION_SCHEM", ColumnType.STRING),
                columnMeta("FUNCTION_NAME", ColumnType.STRING),
                columnMeta("REMARKS", ColumnType.STRING),
                columnMeta("FUNCTION_TYPE", ColumnType.STRING),
                columnMeta("SPECIFIC_NAME", ColumnType.STRING)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPtrn, String functionNamePtrn,
            String colNamePtrn) throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(asList(
                columnMeta("FUNCTION_CAT", ColumnType.STRING),
                columnMeta("FUNCTION_SCHEM", ColumnType.STRING),
                columnMeta("FUNCTION_NAME", ColumnType.STRING),
                columnMeta("COLUMN_NAME", ColumnType.STRING),
                columnMeta("COLUMN_TYPE", ColumnType.INT16),
                columnMeta("DATA_TYPE", ColumnType.INT32),
                columnMeta("TYPE_NAME", ColumnType.STRING),
                columnMeta("PRECISION", ColumnType.INT32),
                columnMeta("LENGTH", ColumnType.INT32),
                columnMeta("SCALE", ColumnType.INT16),
                columnMeta("RADIX", ColumnType.INT16),
                columnMeta("NULLABLE", ColumnType.INT16),
                columnMeta("REMARKS", ColumnType.STRING),
                columnMeta("CHAR_OCTET_LENGTH", ColumnType.INT32),
                columnMeta("ORDINAL_POSITION", ColumnType.INT32),
                columnMeta("IS_NULLABLE", ColumnType.STRING),
                columnMeta("SPECIFIC_NAME", ColumnType.STRING)
        ));
    }

    /** {@inheritDoc} */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface)) {
            throw new SQLException("Database meta data is not a wrapper for " + iface.getName());
        }

        return (T) this;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface != null && iface.isAssignableFrom(JdbcDatabaseMetadata.class);
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPtrn, String tblNamePtrn,
            String colNamePtrn) throws SQLException {
        ensureNotClosed();

        return createObjectListResultSet(asList(
                columnMeta("TABLE_CAT", ColumnType.STRING),
                columnMeta("TABLE_SCHEM", ColumnType.STRING),
                columnMeta("TABLE_NAME", ColumnType.STRING),
                columnMeta("COLUMN_NAME", ColumnType.STRING),
                columnMeta("DATA_TYPE", ColumnType.INT32),
                columnMeta("COLUMN_SIZE", ColumnType.INT32),
                columnMeta("DECIMAL_DIGITS", ColumnType.INT32),
                columnMeta("NUM_PREC_RADIX", ColumnType.INT32),
                columnMeta("COLUMN_USAGE", ColumnType.INT32),
                columnMeta("REMARKS", ColumnType.STRING),
                columnMeta("CHAR_OCTET_LENGTH", ColumnType.INT32),
                columnMeta("IS_NULLABLE", ColumnType.STRING)
        ));
    }

    /**
     * Checks if specified catalog matches the only possible catalog value. See {@link JdbcDatabaseMetadata#CATALOG_NAME}.
     *
     * @param catalog Catalog name or {@code null}.
     * @return {@code true} If catalog equal ignoring case to {@link JdbcDatabaseMetadata#CATALOG_NAME} or null (which means any catalog),
     *         otherwise returns {@code false}.
     */
    private static boolean isValidCatalog(String catalog) {
        return catalog == null || catalog.equalsIgnoreCase(CATALOG_NAME);
    }

    /** {@inheritDoc} */
    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }

    /**
     * Constructs a list of rows in jdbc format for a given table metadata.
     *
     * @param tblMeta Table metadata.
     * @return Table metadata row.
     */
    private List<Object> tableRow(JdbcTableMeta tblMeta) {
        List<Object> row = new ArrayList<>(10);

        row.add(CATALOG_NAME);
        row.add(tblMeta.schemaName());
        row.add(tblMeta.tableName());
        row.add(tblMeta.tableType());
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);

        return row;
    }

    /**
     * Constructs a list of rows in jdbc format for a given column metadata.
     *
     * @param colMeta Column metadata.
     * @param pos Ordinal position.
     * @return Column metadata row.
     */
    public static List<Object> columnRow(JdbcColumnMeta colMeta, int pos) {
        List<Object> row = new ArrayList<>(24);

        row.add(CATALOG_NAME);                  // 1. TABLE_CAT
        row.add(colMeta.schemaName());          // 2. TABLE_SCHEM
        row.add(colMeta.tableName());           // 3. TABLE_NAME
        row.add(colMeta.columnLabel());         // 4. COLUMN_NAME
        row.add(colMeta.dataType());            // 5. DATA_TYPE
        row.add(colMeta.dataTypeName());        // 6. TYPE_NAME
        row.add(colMeta.precision() == -1 ? null : colMeta.precision()); // 7. COLUMN_SIZE
        row.add((Integer) null);                 // 8. BUFFER_LENGTH
        row.add(colMeta.scale() == -1 ? null : colMeta.scale());           // 9. DECIMAL_DIGITS
        row.add(10);                            // 10. NUM_PREC_RADIX
        row.add(colMeta.isNullable() ? columnNullable : columnNoNulls);  // 11. NULLABLE
        row.add((String) null);                  // 12. REMARKS
        row.add(colMeta.defaultValue());        // 13. COLUMN_DEF
        row.add(colMeta.dataType());            // 14. SQL_DATA_TYPE
        row.add((Integer) null);                 // 15. SQL_DATETIME_SUB
        row.add(Integer.MAX_VALUE);             // 16. CHAR_OCTET_LENGTH
        row.add(pos);                           // 17. ORDINAL_POSITION
        row.add(colMeta.isNullable() ? "YES" : "NO"); // 18. IS_NULLABLE
        row.add((String) null);                  // 19. SCOPE_CATALOG
        row.add((String) null);                  // 20. SCOPE_SCHEMA
        row.add((String) null);                  // 21. SCOPE_TABLE
        row.add((Short) null);                   // 22. SOURCE_DATA_TYPE
        row.add("NO");                          // 23. IS_AUTOINCREMENT
        row.add("NO");                          // 23. IS_GENERATEDCOLUMN

        return row;
    }

    /**
     * Constructs a list of rows in jdbc format for a given primary key metadata.
     *
     * @param pkMeta Primary key metadata.
     * @return Result set rows for primary key.
     */
    private static List<List<Object>> primaryKeyRows(JdbcPrimaryKeyMeta pkMeta) {
        List<List<Object>> rows = new ArrayList<>(pkMeta.fields().size());

        for (int i = 0; i < pkMeta.fields().size(); ++i) {
            List<Object> row = new ArrayList<>(6);

            row.add(CATALOG_NAME); // table catalog
            row.add(pkMeta.schemaName());
            row.add(pkMeta.tableName());
            row.add(pkMeta.fields().get(i));
            row.add(i + 1); // sequence number
            row.add(pkMeta.name());

            rows.add(row);
        }

        return rows;
    }

    private void ensureNotClosed() throws SQLException {
        if (connection.isClosed()) {
            throw new SQLException("Connection is closed.", CONNECTION_CLOSED);
        }
    }

    private static ColumnMetadata columnMeta(String name, ColumnType type) {
        return new ColumnMetadataImpl(name, type, -1, -1, true, null);
    }
}
