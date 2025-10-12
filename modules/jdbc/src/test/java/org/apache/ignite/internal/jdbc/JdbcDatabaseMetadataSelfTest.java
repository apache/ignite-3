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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link JdbcDatabaseMetadata}.
 */
public class JdbcDatabaseMetadataSelfTest extends BaseIgniteAbstractTest {

    @Test
    public void constantMethods() throws SQLException {
        String username = String.valueOf(ThreadLocalRandom.current().nextInt(128, 256));
        String jdbcUrl = "jdbc:ignite://localhost:" + ThreadLocalRandom.current().nextInt(1000, 10800);

        JdbcClientQueryEventHandler handler = Mockito.mock(JdbcClientQueryEventHandler.class);
        Connection connection = Mockito.mock(Connection.class);
        DatabaseMetaData metaData = new JdbcDatabaseMetadata(connection, handler, jdbcUrl, username);

        // Basic info
        assertTrue(metaData.allProceduresAreCallable());
        assertTrue(metaData.allTablesAreSelectable());
        assertEquals(jdbcUrl, metaData.getURL());
        assertEquals(username, metaData.getUserName());
        assertFalse(metaData.isReadOnly());

        // Null ordering
        assertFalse(metaData.nullsAreSortedHigh());
        assertTrue(metaData.nullsAreSortedLow());
        assertFalse(metaData.nullsAreSortedAtStart());
        assertTrue(metaData.nullsAreSortedAtEnd());

        // Product and driver info
        assertEquals("Apache Ignite", metaData.getDatabaseProductName());
        assertEquals(ProtocolVersion.LATEST_VER.toString(), metaData.getDatabaseProductVersion());
        assertEquals("Apache Ignite JDBC Driver", metaData.getDriverName());
        IgniteProductVersion productVersion = IgniteProductVersion.CURRENT_VERSION;
        assertEquals(productVersion.toString(), metaData.getDriverVersion());
        assertEquals(productVersion.major(), metaData.getDriverMajorVersion());
        assertEquals(productVersion.minor(), metaData.getDriverMinorVersion());

        // Files, identifiers, quotes
        assertFalse(metaData.usesLocalFiles());
        assertFalse(metaData.usesLocalFilePerTable());
        assertFalse(metaData.supportsMixedCaseIdentifiers());
        assertTrue(metaData.storesUpperCaseIdentifiers());
        assertFalse(metaData.storesLowerCaseIdentifiers());
        assertFalse(metaData.storesMixedCaseIdentifiers());
        assertFalse(metaData.supportsMixedCaseQuotedIdentifiers());
        assertTrue(metaData.storesUpperCaseQuotedIdentifiers());
        assertFalse(metaData.storesLowerCaseQuotedIdentifiers());
        assertTrue(metaData.storesMixedCaseQuotedIdentifiers());
        assertEquals("\"", metaData.getIdentifierQuoteString());
        assertEquals("", metaData.getSQLKeywords());
        assertEquals("", metaData.getNumericFunctions());
        assertEquals("", metaData.getStringFunctions());
        assertEquals("", metaData.getSystemFunctions());
        assertEquals("", metaData.getTimeDateFunctions());
        assertEquals("\\", metaData.getSearchStringEscape());
        assertEquals("", metaData.getExtraNameCharacters());

        // Simple supports flags
        assertTrue(metaData.supportsAlterTableWithAddColumn());
        assertTrue(metaData.supportsAlterTableWithDropColumn());
        assertTrue(metaData.supportsColumnAliasing());
        assertTrue(metaData.nullPlusNonNullIsNull());
        assertFalse(metaData.supportsConvert());
        assertFalse(metaData.supportsConvert(0, 0));
        assertTrue(metaData.supportsTableCorrelationNames());
        assertTrue(metaData.supportsDifferentTableCorrelationNames());
        assertTrue(metaData.supportsExpressionsInOrderBy());
        assertTrue(metaData.supportsOrderByUnrelated());
        assertTrue(metaData.supportsGroupBy());
        assertTrue(metaData.supportsGroupByUnrelated());
        assertTrue(metaData.supportsGroupByBeyondSelect());
        assertTrue(metaData.supportsLikeEscapeClause());
        assertTrue(metaData.supportsMultipleResultSets());
        assertTrue(metaData.supportsMultipleTransactions());
        assertTrue(metaData.supportsNonNullableColumns());

        // SQL support
        assertTrue(metaData.supportsMinimumSQLGrammar());
        assertTrue(metaData.supportsCoreSQLGrammar());
        assertTrue(metaData.supportsExtendedSQLGrammar());
        assertTrue(metaData.supportsANSI92EntryLevelSQL());
        assertTrue(metaData.supportsANSI92IntermediateSQL());
        assertFalse(metaData.supportsANSI92FullSQL());
        assertFalse(metaData.supportsIntegrityEnhancementFacility());
        assertTrue(metaData.supportsOuterJoins());
        assertTrue(metaData.supportsFullOuterJoins());
        assertTrue(metaData.supportsFullOuterJoins());
        assertTrue(metaData.supportsLimitedOuterJoins());

        assertEquals("schema", metaData.getSchemaTerm());
        assertEquals("", metaData.getProcedureTerm());
        assertEquals("", metaData.getCatalogTerm());
        assertFalse(metaData.isCatalogAtStart());
        assertEquals("", metaData.getCatalogSeparator());

        assertTrue(metaData.supportsSchemasInDataManipulation());
        assertFalse(metaData.supportsSchemasInProcedureCalls());
        assertTrue(metaData.supportsSchemasInTableDefinitions());
        assertFalse(metaData.supportsSchemasInIndexDefinitions());
        assertFalse(metaData.supportsSchemasInPrivilegeDefinitions());
        assertFalse(metaData.supportsCatalogsInDataManipulation());
        assertFalse(metaData.supportsCatalogsInProcedureCalls());
        assertFalse(metaData.supportsCatalogsInTableDefinitions());
        assertFalse(metaData.supportsCatalogsInIndexDefinitions());
        assertFalse(metaData.supportsCatalogsInPrivilegeDefinitions());
        assertFalse(metaData.supportsPositionedDelete());
        assertFalse(metaData.supportsPositionedUpdate());
        assertFalse(metaData.supportsSelectForUpdate());
        assertFalse(metaData.supportsStoredProcedures());
        assertTrue(metaData.supportsSubqueriesInComparisons());
        assertTrue(metaData.supportsSubqueriesInExists());
        assertTrue(metaData.supportsSubqueriesInIns());
        assertTrue(metaData.supportsSubqueriesInQuantifieds());
        assertTrue(metaData.supportsCorrelatedSubqueries());
        assertTrue(metaData.supportsUnion());
        assertTrue(metaData.supportsUnionAll());

        // Statements
        assertFalse(metaData.supportsOpenCursorsAcrossCommit());
        assertFalse(metaData.supportsOpenCursorsAcrossRollback());
        assertFalse(metaData.supportsOpenStatementsAcrossCommit());
        assertFalse(metaData.supportsOpenStatementsAcrossCommit());
        assertFalse(metaData.supportsOpenStatementsAcrossRollback());

        // Lengths
        assertEquals(Integer.MAX_VALUE, metaData.getMaxBinaryLiteralLength());
        assertEquals(Integer.MAX_VALUE, metaData.getMaxCharLiteralLength());
        assertEquals(128, metaData.getMaxColumnNameLength());
        assertEquals(0, metaData.getMaxColumnsInGroupBy());
        assertEquals(0, metaData.getMaxColumnsInIndex());
        assertEquals(0, metaData.getMaxColumnsInOrderBy());
        assertEquals(0, metaData.getMaxColumnsInSelect());
        assertEquals(0, metaData.getMaxColumnsInTable());
        assertEquals(0, metaData.getMaxConnections());
        assertEquals(0, metaData.getMaxCursorNameLength());
        assertEquals(0, metaData.getMaxIndexLength());
        assertEquals(128, metaData.getMaxSchemaNameLength());
        assertEquals(0, metaData.getMaxProcedureNameLength());
        assertEquals(0, metaData.getMaxCatalogNameLength());
        assertEquals(0, metaData.getMaxRowSize());
        assertFalse(metaData.doesMaxRowSizeIncludeBlobs());
        assertEquals(0, metaData.getMaxStatementLength());
        assertEquals(0, metaData.getMaxStatements());
        assertEquals(0, metaData.getMaxTablesInSelect());
        assertEquals(0, metaData.getMaxUserNameLength());
        assertEquals(0, metaData.getMaxUserNameLength());

        // Transactions related constants
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, metaData.getDefaultTransactionIsolation());
        assertTrue(metaData.supportsTransactions());

        assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));
        assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
        assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
        assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
        assertTrue(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));

        assertFalse(metaData.supportsDataDefinitionAndDataManipulationTransactions());
        assertFalse(metaData.supportsDataManipulationTransactionsOnly());
        assertFalse(metaData.dataDefinitionCausesTransactionCommit());
        assertFalse(metaData.dataDefinitionIgnoredInTransactions());

        // ResultSet support constants
        assertTrue(metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(metaData.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(metaData.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));

        assertTrue(metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        assertFalse(metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));

        // ResultSet own DML methods
        assertFalse(metaData.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(metaData.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(metaData.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));

        assertFalse(metaData.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(metaData.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(metaData.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));

        assertFalse(metaData.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(metaData.ownInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(metaData.ownInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));

        // ResultSet other DML methods
        assertFalse(metaData.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(metaData.othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(metaData.othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));

        assertFalse(metaData.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(metaData.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(metaData.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));

        assertFalse(metaData.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(metaData.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(metaData.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));

        // ResultSet DML ops detected
        assertFalse(metaData.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(metaData.updatesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(metaData.updatesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));

        assertFalse(metaData.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(metaData.deletesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(metaData.deletesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));

        assertFalse(metaData.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(metaData.insertsAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(metaData.insertsAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));

        // Holdability, JDBC and SQL state meta
        assertTrue(metaData.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, metaData.getResultSetHoldability());
        assertTrue(metaData.getDatabaseMajorVersion() >= 0);
        assertTrue(metaData.getDatabaseMinorVersion() >= 0);
        assertTrue(metaData.getJDBCMajorVersion() >= 0);
        assertTrue(metaData.getJDBCMinorVersion() >= 0);
        assertEquals(DatabaseMetaData.sqlStateSQL99, metaData.getSQLStateType());
        assertFalse(metaData.locatorsUpdateCopy());
        assertFalse(metaData.supportsStatementPooling());
        assertEquals(RowIdLifetime.ROWID_UNSUPPORTED, metaData.getRowIdLifetime());
        assertFalse(metaData.supportsStoredFunctionsUsingCallSyntax());
        assertFalse(metaData.autoCommitFailureClosesAllResultSets());

        // Wrapper and connection
        assertTrue(metaData.isWrapperFor(JdbcDatabaseMetadata.class));
        assertSame(connection, metaData.getConnection());
        assertSame(metaData, metaData.unwrap(JdbcDatabaseMetadata.class));
        assertSame(metaData, metaData.unwrap(DatabaseMetaData.class));

        // Misc flags
        assertTrue(metaData.supportsBatchUpdates());
        assertFalse(metaData.supportsSavepoints());
        assertFalse(metaData.supportsNamedParameters());
        assertFalse(metaData.supportsMultipleOpenResults());
        assertFalse(metaData.supportsGetGeneratedKeys());

        assertFalse(metaData.generatedKeyAlwaysReturned());
    }
}
