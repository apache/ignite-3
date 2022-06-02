package org.apache.ignite.cli.commands.sql;

import static org.apache.ignite.cli.core.exception.handler.SqlExceptionHandler.CLIENT_CONNECTION_FAILED_MESSAGE;
import static org.apache.ignite.cli.core.exception.handler.SqlExceptionHandler.PARSING_ERROR_MESSAGE;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.cli.commands.CliCommandTestIntegrationBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for {@link SqlCommand}.
 */
class ItSqlCommandTest extends CliCommandTestIntegrationBase {

    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        createAndPopulateTable();
    }

    @AfterEach
    void tearDown() {
        dropAllTables();
    }

    @Test
    @DisplayName("Should execute select * from table and display table when jdbc-url is correct")
    void selectFromTable() {
        execute("sql", "--execute", "select * from person", "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertOutputIsNotEmpty,
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should display readable error when wrong jdbc is given")
    void wrongJdbcUrl() {
        execute("sql", "--execute", "select * from person", "--jdbc-url", "jdbc:ignite:thin://no-such-host.com:10800");

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                this::assertErrOutputIsNotEmpty,
                // todo: specify error output
                () -> assertErrOutputIs(CLIENT_CONNECTION_FAILED_MESSAGE + System.lineSeparator())
        );
    }

    @Test
    @DisplayName("Should display readable error when wrong query is given")
    void incorrectQueryTest() {
        execute("sql", "--execute", "select", "--jdbc-url", JDBC_URL);

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                this::assertErrOutputIsNotEmpty,
                // todo: specify error output
                () -> assertErrOutputIs(PARSING_ERROR_MESSAGE + System.lineSeparator())
        );
    }
}