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

package org.apache.ignite.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.client.proto.ProtocolVersion;
import org.apache.ignite.client.proto.query.event.JdbcColumnMeta;
import org.apache.ignite.client.proto.query.event.JdbcMetaColumnsRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaColumnsResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaTablesRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaTablesResult;
import org.apache.ignite.client.proto.query.event.JdbcTableMeta;

import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.RowIdLifetime.ROWID_UNSUPPORTED;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * JDBC database metadata implementation.
 */
public class JdbcDatabaseMetadata implements DatabaseMetaData {
    /** Driver name. */
    public static final String DRIVER_NAME = "Apache Ignite JDBC Driver";

    /** The only possible name for catalog. */
    public static final String CATALOG_NAME = "IGNITE";

    /** Name of TABLE type. */
    public static final String TYPE_TABLE = "TABLE";

    /** Name of VIEW type. */
    public static final String TYPE_VIEW = "VIEW";

    /** Connection. */
    private final JdbcConnection conn;

    /**
     * @param conn Connection.
     */
    JdbcDatabaseMetadata(JdbcConnection conn) {
        this.conn = conn;
    }

    /** {@inheritDoc} */
    @Override public boolean allProceduresAreCallable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean allTablesAreSelectable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String getURL() {
        return conn.url();
    }

    /** {@inheritDoc} */
    @Override public String getUserName() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public boolean isReadOnly() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean nullsAreSortedHigh() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean nullsAreSortedLow() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean nullsAreSortedAtStart() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean nullsAreSortedAtEnd() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String getDatabaseProductName() {
        return "Apache Ignite";
    }

    /** {@inheritDoc} */
    @Override public String getDatabaseProductVersion() {
        return ProtocolVersion.LATEST_VER.toString();
    }

    /** {@inheritDoc} */
    @Override public String getDriverName() {
        return DRIVER_NAME;
    }

    /** {@inheritDoc} */
    @Override public String getDriverVersion() {
        return ProtocolVersion.LATEST_VER.toString();
    }

    /** {@inheritDoc} */
    @Override public int getDriverMajorVersion() {
        return ProtocolVersion.LATEST_VER.major();
    }

    /** {@inheritDoc} */
    @Override public int getDriverMinorVersion() {
        return ProtocolVersion.LATEST_VER.minor();
    }

    /** {@inheritDoc} */
    @Override public boolean usesLocalFiles() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean usesLocalFilePerTable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMixedCaseIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean storesUpperCaseIdentifiers() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean storesMixedCaseIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMixedCaseQuotedIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean storesUpperCaseQuotedIdentifiers() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean storesMixedCaseQuotedIdentifiers() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String getIdentifierQuoteString() {
        return "\"";
    }

    /** {@inheritDoc} */
    @Override public String getSQLKeywords() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getNumericFunctions() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getStringFunctions() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getSystemFunctions() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getTimeDateFunctions() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getSearchStringEscape() {
        return "\\";
    }

    /** {@inheritDoc} */
    @Override public String getExtraNameCharacters() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public boolean supportsAlterTableWithAddColumn() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsAlterTableWithDropColumn() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsColumnAliasing() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean nullPlusNonNullIsNull() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsConvert() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsConvert(int fromType, int toType) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsTableCorrelationNames() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsDifferentTableCorrelationNames() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOrderByUnrelated() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsGroupBy() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsGroupByUnrelated() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsLikeEscapeClause() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMultipleResultSets() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMultipleTransactions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsNonNullableColumns() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMinimumSQLGrammar() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCoreSQLGrammar() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsANSI92EntryLevelSQL() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsANSI92FullSQL() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOuterJoins() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsFullOuterJoins() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsLimitedOuterJoins() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String getSchemaTerm() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getProcedureTerm() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getCatalogTerm() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public boolean isCatalogAtStart() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String getCatalogSeparator() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSchemasInDataManipulation() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSchemasInTableDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCatalogsInDataManipulation() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsPositionedDelete() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsPositionedUpdate() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSelectForUpdate() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsStoredProcedures() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSubqueriesInComparisons() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSubqueriesInExists() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSubqueriesInIns() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSubqueriesInQuantifieds() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsUnion() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsUnionAll() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOpenStatementsAcrossCommit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOpenStatementsAcrossRollback() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int getMaxBinaryLiteralLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxCharLiteralLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnsInGroupBy() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnsInIndex() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnsInOrderBy() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnsInSelect() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnsInTable() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxConnections() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxCursorNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxIndexLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxSchemaNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxProcedureNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxCatalogNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxRowSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean doesMaxRowSizeIncludeBlobs() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int getMaxStatementLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxStatements() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxTableNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxTablesInSelect() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxUserNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getDefaultTransactionIsolation() {
        return TRANSACTION_NONE;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsTransactions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsTransactionIsolationLevel(int level) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getProcedures(String catalog, String schemaPtrn,
        String procedureNamePtrn) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "PROCEDURE_CAT", String.class),
            new JdbcColumnMeta(null, null, "PROCEDURE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "PROCEDURE_NAME", String.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "PROCEDURE_TYPE", String.class),
            new JdbcColumnMeta(null, null, "SPECIFIC_NAME", String.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getProcedureColumns(String catalog, String schemaPtrn, String procedureNamePtrn,
        String colNamePtrn) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "PROCEDURE_CAT", String.class),
            new JdbcColumnMeta(null, null, "PROCEDURE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "PROCEDURE_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_TYPE", Short.class),
            new JdbcColumnMeta(null, null, "COLUMN_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "PRECISION", Integer.class),
            new JdbcColumnMeta(null, null, "LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "SCALE", Short.class),
            new JdbcColumnMeta(null, null, "RADIX", Short.class),
            new JdbcColumnMeta(null, null, "NULLABLE", Short.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_DEF", String.class),
            new JdbcColumnMeta(null, null, "SQL_DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "SQL_DATETIME_SUB", Integer.class),
            new JdbcColumnMeta(null, null, "CHAR_OCTET_LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "ORDINAL_POSITION", Integer.class),
            new JdbcColumnMeta(null, null, "IS_NULLABLE", String.class),
            new JdbcColumnMeta(null, null, "SPECIFIC_NAME", String.class)
            ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTables(String catalog, String schemaPtrn, String tblNamePtrn, String[] tblTypes)
        throws SQLException {
        conn.ensureNotClosed();

        final List<JdbcColumnMeta> meta = asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "TABLE_TYPE", String.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "TYPE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TYPE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "SELF_REFERENCING_COL_NAME", String.class),
            new JdbcColumnMeta(null, null, "REF_GENERATION", String.class));

        boolean tblTypeMatch = false;

        if (tblTypes == null)
            tblTypeMatch = true;
        else {
            for (String type : tblTypes) {
                if (TYPE_TABLE.equals(type) || TYPE_VIEW.equals(type)) {
                    tblTypeMatch = true;

                    break;
                }
            }
        }

        if (!isValidCatalog(catalog) || !tblTypeMatch)
            return new JdbcResultSet(Collections.emptyList(), meta);

        JdbcMetaTablesResult res
            = conn.handler().tablesMeta(new JdbcMetaTablesRequest(schemaPtrn, tblNamePtrn, tblTypes));

        List<List<Object>> rows = new LinkedList<>();

        for (JdbcTableMeta tblMeta : res.meta())
            rows.add(tableRow(tblMeta));

        return new JdbcResultSet(rows, meta);
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, "%");
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    @Override public ResultSet getCatalogs() {
        return new JdbcResultSet(singletonList(singletonList(CATALOG_NAME)),
            asList(new JdbcColumnMeta(null, null, "TABLE_CAT", String.class)));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    @Override public ResultSet getTableTypes() {
        return new JdbcResultSet(
            asList(singletonList(TYPE_TABLE), singletonList(TYPE_VIEW) ),
            asList(new JdbcColumnMeta(null, null, "TABLE_TYPE", String.class)));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getColumns(String catalog, String schemaPtrn, String tblNamePtrn, String colNamePtrn) throws SQLException {
        conn.ensureNotClosed();

        final List<JdbcColumnMeta> meta = asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),      // 1
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),    // 2
            new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),     // 3
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),    // 4
            new JdbcColumnMeta(null, null, "DATA_TYPE", Short.class),       // 5
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),      // 6
            new JdbcColumnMeta(null, null, "COLUMN_SIZE", Integer.class),   // 7
            new JdbcColumnMeta(null, null, "BUFFER_LENGTH", Integer.class), // 8
            new JdbcColumnMeta(null, null, "DECIMAL_DIGITS", Integer.class), // 9
            new JdbcColumnMeta(null, null, "NUM_PREC_RADIX", Short.class),  // 10
            new JdbcColumnMeta(null, null, "NULLABLE", Short.class),        // 11
            new JdbcColumnMeta(null, null, "REMARKS", String.class),        // 12
            new JdbcColumnMeta(null, null, "COLUMN_DEF", String.class),     // 13
            new JdbcColumnMeta(null, null, "SQL_DATA_TYPE", Integer.class), // 14
            new JdbcColumnMeta(null, null, "SQL_DATETIME_SUB", Integer.class), // 15
            new JdbcColumnMeta(null, null, "CHAR_OCTET_LENGTH", Integer.class), // 16
            new JdbcColumnMeta(null, null, "ORDINAL_POSITION", Integer.class), // 17
            new JdbcColumnMeta(null, null, "IS_NULLABLE", String.class),    // 18
            new JdbcColumnMeta(null, null, "SCOPE_CATLOG", String.class),   // 19
            new JdbcColumnMeta(null, null, "SCOPE_SCHEMA", String.class),   // 20
            new JdbcColumnMeta(null, null, "SCOPE_TABLE", String.class),    // 21
            new JdbcColumnMeta(null, null, "SOURCE_DATA_TYPE", Short.class), // 22
            new JdbcColumnMeta(null, null, "IS_AUTOINCREMENT", String.class), // 23
            new JdbcColumnMeta(null, null, "IS_GENERATEDCOLUMN", String.class) // 24
        );

        if (!isValidCatalog(catalog))
            return new JdbcResultSet(Collections.emptyList(), meta);

        JdbcMetaColumnsResult res = conn.handler().columnsMeta(new JdbcMetaColumnsRequest(schemaPtrn, tblNamePtrn, colNamePtrn));

        List<List<Object>> rows = new LinkedList<>();

        for (int i = 0; i < res.meta().size(); ++i)
            rows.add(columnRow(res.meta().get(i), i + 1));

        return new JdbcResultSet(rows, meta);
    }

    /** {@inheritDoc} */
    @Override public ResultSet getColumnPrivileges(String catalog, String schema, String tbl,
        String colNamePtrn) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "GRANTOR", String.class),
            new JdbcColumnMeta(null, null, "GRANTEE", String.class),
            new JdbcColumnMeta(null, null, "PRIVILEGE", String.class),
            new JdbcColumnMeta(null, null, "IS_GRANTABLE", String.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTablePrivileges(String catalog, String schemaPtrn,
        String tblNamePtrn) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "GRANTOR", String.class),
            new JdbcColumnMeta(null, null, "GRANTEE", String.class),
            new JdbcColumnMeta(null, null, "PRIVILEGE", String.class),
            new JdbcColumnMeta(null, null, "IS_GRANTABLE", String.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getBestRowIdentifier(String catalog, String schema, String tbl, int scope,
        boolean nullable) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "SCOPE", Short.class),
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_SIZE", Integer.class),
            new JdbcColumnMeta(null, null, "BUFFER_LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "DECIMAL_DIGITS", Short.class),
            new JdbcColumnMeta(null, null, "PSEUDO_COLUMN", Short.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getVersionColumns(String catalog, String schema, String tbl) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "SCOPE", Short.class),
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_SIZE", Integer.class),
            new JdbcColumnMeta(null, null, "BUFFER_LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "DECIMAL_DIGITS", Short.class),
            new JdbcColumnMeta(null, null, "PSEUDO_COLUMN", Short.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getPrimaryKeys(String catalog, String schema, String tbl) throws SQLException {
        conn.ensureNotClosed();

        final List<JdbcColumnMeta> meta = asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "KEY_SEQ", Short.class),
            new JdbcColumnMeta(null, null, "PK_NAME", String.class));

        if (!isValidCatalog(catalog))
            return new JdbcResultSet(Collections.emptyList(), meta);
//
//        JdbcMetaPrimaryKeysResult res = conn.sendRequest(new JdbcMetaPrimaryKeysRequest(schema, tbl)).
//            response();
//
//        List<List<Object>> rows = new LinkedList<>();
//
//        for (JdbcPrimaryKeyMeta pkMeta : res.meta())
//            rows.addAll(primaryKeyRows(pkMeta));
//
//        return new JdbcResultSet(rows, meta);
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getImportedKeys(String catalog, String schema, String tbl) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "PKTABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "PKTABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "PKTABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "PKCOLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "FKCOLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "KEY_SEQ", Short.class),
            new JdbcColumnMeta(null, null, "UPDATE_RULE", Short.class),
            new JdbcColumnMeta(null, null, "DELETE_RULE", Short.class),
            new JdbcColumnMeta(null, null, "FK_NAME", String.class),
            new JdbcColumnMeta(null, null, "PK_NAME", String.class),
            new JdbcColumnMeta(null, null, "DEFERRABILITY", Short.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getExportedKeys(String catalog, String schema, String tbl) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "PKTABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "PKTABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "PKTABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "PKCOLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "FKCOLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "KEY_SEQ", Short.class),
            new JdbcColumnMeta(null, null, "UPDATE_RULE", Short.class),
            new JdbcColumnMeta(null, null, "DELETE_RULE", Short.class),
            new JdbcColumnMeta(null, null, "FK_NAME", String.class),
            new JdbcColumnMeta(null, null, "PK_NAME", String.class),
            new JdbcColumnMeta(null, null, "DEFERRABILITY", Short.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTbl,
        String foreignCatalog, String foreignSchema, String foreignTbl) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "PKTABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "PKTABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "PKTABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "PKCOLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "FKCOLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "KEY_SEQ", Short.class),
            new JdbcColumnMeta(null, null, "UPDATE_RULE", Short.class),
            new JdbcColumnMeta(null, null, "DELETE_RULE", Short.class),
            new JdbcColumnMeta(null, null, "FK_NAME", String.class),
            new JdbcColumnMeta(null, null, "PK_NAME", String.class),
            new JdbcColumnMeta(null, null, "DEFERRABILITY", Short.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTypeInfo() {
        List<List<Object>> types = new ArrayList<>(21);

        types.add(Arrays.asList("BOOLEAN", Types.BOOLEAN, 1, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "BOOLEAN", 0, 0,
            Types.BOOLEAN, 0, 10));

        types.add(Arrays.asList("TINYINT", Types.TINYINT, 3, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "TINYINT", 0, 0,
            Types.TINYINT, 0, 10));

        types.add(Arrays.asList("SMALLINT", Types.SMALLINT, 5, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "SMALLINT", 0, 0,
            Types.SMALLINT, 0, 10));

        types.add(Arrays.asList("INTEGER", Types.INTEGER, 10, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "INTEGER", 0, 0,
            Types.INTEGER, 0, 10));

        types.add(Arrays.asList("BIGINT", Types.BIGINT, 19, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "BIGINT", 0, 0,
            Types.BIGINT, 0, 10));

        types.add(Arrays.asList("FLOAT", Types.FLOAT, 17, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "FLOAT", 0, 0,
            Types.FLOAT, 0, 10));

        types.add(Arrays.asList("REAL", Types.REAL, 7, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "REAL", 0, 0,
            Types.REAL, 0, 10));

        types.add(Arrays.asList("DOUBLE", Types.DOUBLE, 17, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "DOUBLE", 0, 0,
            Types.DOUBLE, 0, 10));

        types.add(Arrays.asList("NUMERIC", Types.NUMERIC, Integer.MAX_VALUE, null, null, "PRECISION,SCALE",
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "NUMERIC", 0, 0,
            Types.NUMERIC, 0, 10));

        types.add(Arrays.asList("DECIMAL", Types.DECIMAL, Integer.MAX_VALUE, null, null, "PRECISION,SCALE",
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "DECIMAL", 0, 0,
            Types.DECIMAL, 0, 10));

        types.add(Arrays.asList("DATE", Types.DATE, 8, "DATE '", "'", null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "DATE", 0, 0,
            Types.DATE, 0, null));

        types.add(Arrays.asList("TIME", Types.TIME, 6, "TIME '", "'", null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "TIME", 0, 0,
            Types.TIME, 0, null));

        types.add(Arrays.asList("TIMESTAMP", Types.TIMESTAMP, 23, "TIMESTAMP '", "'", null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "TIMESTAMP", 0, 10,
            Types.TIMESTAMP, 0, null));

        types.add(Arrays.asList("CHAR", Types.CHAR, Integer.MAX_VALUE, "'", "'", "LENGTH",
            (short)typeNullable, true, (short)typeSearchable, false, false, false, "CHAR", 0, 0,
            Types.CHAR, 0, null));

        types.add(Arrays.asList("VARCHAR", Types.VARCHAR, Integer.MAX_VALUE, "'", "'", "LENGTH",
            (short)typeNullable, true, (short)typeSearchable, false, false, false, "VARCHAR", 0, 0,
            Types.VARCHAR, 0, null));

        types.add(Arrays.asList("LONGVARCHAR", Types.LONGVARCHAR, Integer.MAX_VALUE, "'", "'", "LENGTH",
            (short)typeNullable, true, (short)typeSearchable, false, false, false, "LONGVARCHAR", 0, 0,
            Types.LONGVARCHAR, 0, null));

        types.add(Arrays.asList("BINARY", Types.BINARY, Integer.MAX_VALUE, "'", "'", "LENGTH",
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "BINARY", 0, 0,
            Types.BINARY, 0, null));

        types.add(Arrays.asList("VARBINARY", Types.VARBINARY, Integer.MAX_VALUE, "'", "'", "LENGTH",
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "VARBINARY", 0, 0,
            Types.VARBINARY, 0, null));

        types.add(Arrays.asList("LONGVARBINARY", Types.LONGVARBINARY, Integer.MAX_VALUE, "'", "'", "LENGTH",
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "LONGVARBINARY", 0, 0,
            Types.LONGVARBINARY, 0, null));

        types.add(Arrays.asList("OTHER", Types.OTHER, Integer.MAX_VALUE, "'", "'", "LENGTH",
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "OTHER", 0, 0,
            Types.OTHER, 0, null));

        types.add(Arrays.asList("ARRAY", Types.ARRAY, 0, "(", "')", null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "ARRAY", 0, 0,
            Types.ARRAY, 0, null));

        return new JdbcResultSet(types, asList(
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "PRECISION", Integer.class),
            new JdbcColumnMeta(null, null, "LITERAL_PREFIX", String.class),
            new JdbcColumnMeta(null, null, "LITERAL_SUFFIX", String.class),
            new JdbcColumnMeta(null, null, "CREATE_PARAMS", String.class),
            new JdbcColumnMeta(null, null, "NULLABLE", Short.class),
            new JdbcColumnMeta(null, null, "CASE_SENSITIVE", Boolean.class),
            new JdbcColumnMeta(null, null, "SEARCHABLE", Short.class),
            new JdbcColumnMeta(null, null, "UNSIGNED_ATTRIBUTE", Boolean.class),
            new JdbcColumnMeta(null, null, "FIXED_PREC_SCALE", Boolean.class),
            new JdbcColumnMeta(null, null, "AUTO_INCREMENT", Boolean.class),
            new JdbcColumnMeta(null, null, "LOCAL_TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "MINIMUM_SCALE", Short.class),
            new JdbcColumnMeta(null, null, "MAXIMUM_SCALE", Short.class),
            new JdbcColumnMeta(null, null, "SQL_DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "SQL_DATETIME_SUB", Integer.class),
            new JdbcColumnMeta(null, null, "NUM_PREC_RADIX", Integer.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getIndexInfo(String catalog, String schema, String tbl, boolean unique,
        boolean approximate) throws SQLException {
        conn.ensureNotClosed();

        final List<JdbcColumnMeta> meta = asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "NON_UNIQUE", Boolean.class),
            new JdbcColumnMeta(null, null, "INDEX_QUALIFIER", String.class),
            new JdbcColumnMeta(null, null, "INDEX_NAME", String.class),
            new JdbcColumnMeta(null, null, "TYPE", Short.class),
            new JdbcColumnMeta(null, null, "ORDINAL_POSITION", Short.class),
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "ASC_OR_DESC", String.class),
            new JdbcColumnMeta(null, null, "CARDINALITY", Integer.class),
            new JdbcColumnMeta(null, null, "PAGES", Integer.class),
            new JdbcColumnMeta(null, null, "FILTER_CONDITION", String.class));

        if (!isValidCatalog(catalog))
            return new JdbcResultSet(Collections.emptyList(), meta);

//        JdbcMetaIndexesResult res = conn.sendRequest(new JdbcMetaIndexesRequest(schema, tbl)).response();
//
//        List<List<Object>> rows = new LinkedList<>();
//
//        for (JdbcIndexMeta idxMeta : res.meta())
//            rows.addAll(indexRows(idxMeta));
//
//        return new JdbcResultSet(rows, meta);
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsResultSetType(int type) {
        return type == TYPE_FORWARD_ONLY;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return supportsResultSetType(type) && concurrency == CONCUR_READ_ONLY;
    }

    /** {@inheritDoc} */
    @Override public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean updatesAreDetected(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean deletesAreDetected(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean insertsAreDetected(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsBatchUpdates() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getUDTs(String catalog, String schemaPtrn, String typeNamePtrn,
        int[] types) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "TYPE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TYPE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "CLASS_NAME", String.class),
            new JdbcColumnMeta(null, null, "DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "BASE_TYPE", Short.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public Connection getConnection() {
        return conn;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSavepoints() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsNamedParameters() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMultipleOpenResults() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsGetGeneratedKeys() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSuperTypes(String catalog, String schemaPtrn,
        String typeNamePtrn) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "TYPE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TYPE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "SUPERTYPE_CAT", String.class),
            new JdbcColumnMeta(null, null, "SUPERTYPE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "SUPERTYPE_NAME", String.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSuperTables(String catalog, String schemaPtrn,
        String tblNamePtrn) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "SUPERTABLE_NAME", String.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getAttributes(String catalog, String schemaPtrn, String typeNamePtrn,
        String attributeNamePtrn) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "TYPE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TYPE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "ATTR_NAME", String.class),
            new JdbcColumnMeta(null, null, "DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "ATTR_TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "ATTR_SIZE", Integer.class),
            new JdbcColumnMeta(null, null, "DECIMAL_DIGITS", Integer.class),
            new JdbcColumnMeta(null, null, "NUM_PREC_RADIX", Integer.class),
            new JdbcColumnMeta(null, null, "NULLABLE", Integer.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "ATTR_DEF", String.class),
            new JdbcColumnMeta(null, null, "SQL_DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "SQL_DATETIME_SUB", Integer.class),
            new JdbcColumnMeta(null, null, "CHAR_OCTET_LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "ORDINAL_POSITION", Integer.class),
            new JdbcColumnMeta(null, null, "IS_NULLABLE", String.class),
            new JdbcColumnMeta(null, null, "SCOPE_CATALOG", String.class),
            new JdbcColumnMeta(null, null, "SCOPE_SCHEMA", String.class),
            new JdbcColumnMeta(null, null, "SCOPE_TABLE", String.class),
            new JdbcColumnMeta(null, null, "SOURCE_DATA_TYPE", Short.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public boolean supportsResultSetHoldability(int holdability) {
        return holdability == HOLD_CURSORS_OVER_COMMIT;
    }

    /** {@inheritDoc} */
    @Override public int getResultSetHoldability() {
        return HOLD_CURSORS_OVER_COMMIT;
    }

    /** {@inheritDoc} */
    @Override public int getDatabaseMajorVersion() {
        return ProtocolVersion.LATEST_VER.major();
    }

    /** {@inheritDoc} */
    @Override public int getDatabaseMinorVersion() {
        return ProtocolVersion.LATEST_VER.minor();
    }

    /** {@inheritDoc} */
    @Override public int getJDBCMajorVersion() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override public int getJDBCMinorVersion() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getSQLStateType() {
        return DatabaseMetaData.sqlStateSQL99;
    }

    /** {@inheritDoc} */
    @Override public boolean locatorsUpdateCopy() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsStatementPooling() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public RowIdLifetime getRowIdLifetime() {
        return ROWID_UNSUPPORTED;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSchemas(String catalog, String schemaPtrn) throws SQLException {
        conn.ensureNotClosed();

        final List<JdbcColumnMeta> meta = asList(
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TABLE_CATALOG", String.class)
        );

        if (!isValidCatalog(catalog))
            return new JdbcResultSet(Collections.emptyList(), meta);

//        JdbcMetaSchemasResult res = conn.sendRequest(new JdbcMetaSchemasRequest(schemaPtrn)).response();
//
//        List<List<Object>> rows = new LinkedList<>();
//
//        for (String schema : res.schemas()) {
//            List<Object> row = new ArrayList<>(2);
//
//            row.add(schema);
//            row.add(CATALOG_NAME);
//
//            rows.add(row);
//        }
//
//        return new JdbcResultSet(rows, meta);
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getClientInfoProperties() {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "NAME", String.class),
            new JdbcColumnMeta(null, null, "MAX_LEN", Integer.class),
            new JdbcColumnMeta(null, null, "DEFAULT_VALUE", String.class),
            new JdbcColumnMeta(null, null, "DESCRIPTION", String.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getFunctions(String catalog, String schemaPtrn,
        String functionNamePtrn) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "FUNCTION_CAT", String.class),
            new JdbcColumnMeta(null, null, "FUNCTION_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "FUNCTION_NAME", String.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "FUNCTION_TYPE", String.class),
            new JdbcColumnMeta(null, null, "SPECIFIC_NAME", String.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getFunctionColumns(String catalog, String schemaPtrn, String functionNamePtrn,
        String colNamePtrn) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "FUNCTION_CAT", String.class),
            new JdbcColumnMeta(null, null, "FUNCTION_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "FUNCTION_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_TYPE", Short.class),
            new JdbcColumnMeta(null, null, "DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "PRECISION", Integer.class),
            new JdbcColumnMeta(null, null, "LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "SCALE", Short.class),
            new JdbcColumnMeta(null, null, "RADIX", Short.class),
            new JdbcColumnMeta(null, null, "NULLABLE", Short.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "CHAR_OCTET_LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "ORDINAL_POSITION", Integer.class),
            new JdbcColumnMeta(null, null, "IS_NULLABLE", String.class),
            new JdbcColumnMeta(null, null, "SPECIFIC_NAME", String.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface))
            throw new SQLException("Database meta data is not a wrapper for " + iface.getName());

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) {
        return iface != null && iface.isAssignableFrom(JdbcDatabaseMetadata.class);
    }

    /** {@inheritDoc} */
    @Override public ResultSet getPseudoColumns(String catalog, String schemaPtrn, String tblNamePtrn,
        String colNamePtrn) {
        return new JdbcResultSet(Collections.emptyList(), asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "COLUMN_SIZE", Integer.class),
            new JdbcColumnMeta(null, null, "DECIMAL_DIGITS", Integer.class),
            new JdbcColumnMeta(null, null, "NUM_PREC_RADIX", Integer.class),
            new JdbcColumnMeta(null, null, "COLUMN_USAGE", Integer.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "CHAR_OCTET_LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "IS_NULLABLE", String.class)
        ));
    }

    /**
     * Checks if specified catalog matches the only possible catalog value. See {@link CATALOG_NAME}.
     *
     * @param catalog Catalog name or {@code null}.
     * @return {@code true} If catalog equal ignoring case to {@link CATALOG_NAME} or null (which means any catalog).
     *  Otherwise returns {@code false}.
     */
    private static boolean isValidCatalog(String catalog) {
        return catalog == null || catalog.equalsIgnoreCase(CATALOG_NAME);
    }

    /** {@inheritDoc} */
    @Override public boolean generatedKeyAlwaysReturned() {
        return false;
    }

    /**
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
     * @param colMeta Column metadata.
     * @param pos Ordinal position.
     * @return Column metadata row.
     */
    public static List<Object> columnRow(JdbcColumnMeta colMeta, int pos) {
        List<Object> row = new ArrayList<>(24);

        row.add(CATALOG_NAME);                  // 1. TABLE_CAT
        row.add(colMeta.schemaName());          // 2. TABLE_SCHEM
        row.add(colMeta.tableName());           // 3. TABLE_NAME
        row.add(colMeta.columnName());          // 4. COLUMN_NAME
        row.add(colMeta.dataType());            // 5. DATA_TYPE
        row.add(colMeta.dataTypeName());        // 6. TYPE_NAME
        row.add(colMeta.precision() == -1 ? null : colMeta.precision()); // 7. COLUMN_SIZE
        row.add((Integer)null);                 // 8. BUFFER_LENGTH
        row.add(colMeta.scale() == -1 ? null : colMeta.scale());           // 9. DECIMAL_DIGITS
        row.add(10);                            // 10. NUM_PREC_RADIX
        row.add(colMeta.isNullable() ? columnNullable : columnNoNulls);  // 11. NULLABLE
        row.add((String)null);                  // 12. REMARKS
        row.add(colMeta.defaultValue());        // 13. COLUMN_DEF
        row.add(colMeta.dataType());            // 14. SQL_DATA_TYPE
        row.add((Integer)null);                 // 15. SQL_DATETIME_SUB
        row.add(Integer.MAX_VALUE);             // 16. CHAR_OCTET_LENGTH
        row.add(pos);                           // 17. ORDINAL_POSITION
        row.add(colMeta.isNullable() ? "YES" : "NO"); // 18. IS_NULLABLE
        row.add((String)null);                  // 19. SCOPE_CATALOG
        row.add((String)null);                  // 20. SCOPE_SCHEMA
        row.add((String)null);                  // 21. SCOPE_TABLE
        row.add((Short)null);                   // 22. SOURCE_DATA_TYPE
        row.add("NO");                          // 23. IS_AUTOINCREMENT
        row.add("NO");                          // 23. IS_GENERATEDCOLUMN

        return row;
    }
}
