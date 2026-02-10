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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.QueryEventsFactory.FieldNames;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.tx.Transaction;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests to check the {@link IgniteEventType#QUERY_STARTED} and {@link IgniteEventType#QUERY_FINISHED} events.
 */
public class ItSqlQueryEventLogTest extends BaseSqlIntegrationTest {
    private static final String UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    private static Path eventlogPath;

    @BeforeAll
    static void setUp() {
        String buildDirPath = System.getProperty("buildDirPath");
        eventlogPath = Path.of(buildDirPath).resolve("event.log");

        sql("CREATE TABLE test(id INT PRIMARY KEY)");
    }

    @BeforeEach
    void cleanup() throws IOException {
        List<String> events = readEventLog();

        sql("DELETE FROM test");

        readEvents(events.size() + 2);

        resetLog();
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        String eventLog = "eventlog {\n"
                + " sinks.logSink.channel: testChannel,\n"
                + " channels.testChannel.events:"
                + " [" + IgniteEventType.QUERY_STARTED + ", " + IgniteEventType.QUERY_FINISHED + "],\n"
                + "}\n";

        builder.clusterConfiguration("ignite {\n" + eventLog + "}");
    }

    @Test
    void testQuery() {
        String query = "SELECT 1";
        sql(query);

        List<String> events = readEvents(2);

        UUID queryId = verifyQueryStartedFields(events.get(0), query, null);
        verifyQueryFinishFields(events.get(1), queryId, SqlQueryType.QUERY, query, true, null);
    }

    @Test
    void testMultiStatementQuery() {
        String scriptText = "INSERT INTO test VALUES (0); SELECT 1;";
        igniteSql().executeScript(scriptText);

        List<String> events = readEvents(6);

        UUID scriptId = verifyQueryStartedFields(events.get(0), scriptText, null);

        String insertStatementText = "INSERT INTO test VALUES (0);";
        String selectStatementText = "SELECT 1;";

        UUID insertQueryId = verifyQueryStartedFields(events.get(1), insertStatementText, null, scriptId, 0);

        String selectStartEvent;
        String insertFinishEvent;

        if (events.get(2).startsWith("{\"type\":\"QUERY_STARTED\"")) {
            selectStartEvent = events.get(2);
            insertFinishEvent = events.get(3);
        } else {
            selectStartEvent = events.get(3);
            insertFinishEvent = events.get(2);
        }

        verifyQueryFinishFields(insertFinishEvent, insertQueryId, SqlQueryType.DML, insertStatementText, null, true,
                null, scriptId, 0);

        UUID selectQueryId = verifyQueryStartedFields(selectStartEvent, selectStatementText, null, scriptId, 1);

        // The order in which the last `SELECT` statement finish and script statement finish events are sent is undefined.
        String selectFinishEvent;
        String scriptFinishEvent;

        if (events.get(4).contains(selectQueryId.toString())) {
            selectFinishEvent = events.get(4);
            scriptFinishEvent = events.get(5);
        } else {
            selectFinishEvent = events.get(5);
            scriptFinishEvent = events.get(4);
        }

        verifyQueryFinishFields(selectFinishEvent, selectQueryId, SqlQueryType.QUERY,
                selectStatementText, null, true, null, scriptId, 1);

        verifyQueryFinishFields(scriptFinishEvent, scriptId, null,
                scriptText, null, false, null, null, -1);
    }

    @Test
    void testErrorResetOnTxRetry() throws IOException {
        sql("CREATE TABLE my (id INT PRIMARY KEY, val INT)");
        sql("INSERT INTO my VALUES (1, 0), (2, 0), (3, 0), (4, 0)");

        readEvents(4);
        resetLog();

        int parties = 2;
        Phaser phaser = new Phaser(parties);

        List<CompletableFuture<?>> results = new ArrayList<>(parties);
        for (int i = 0; i < parties; i++) {
            int newValue = i + 1;
            results.add(runAsync(() -> {
                phaser.awaitAdvanceInterruptibly(phaser.arrive());

                sql("UPDATE my SET val = ?", newValue);
            }));
        }

        // all queries are expected to complete successfully
        await(CompletableFutures.allOf(results));

        List<String> events = readEvents(4);

        {
            FieldsChecker fieldsChecker = EventValidator.parseQueryFinish(events.get(2));
            fieldsChecker.verify(FieldNames.ERROR, null);
        }

        {
            FieldsChecker fieldsChecker = EventValidator.parseQueryFinish(events.get(3));
            fieldsChecker.verify(FieldNames.ERROR, null);
        }
    }

    @Test
    public void testParseError() {
        String query = "DELETE * FROM TEST";

        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query: Encountered \"*\" at line 1, column 8",
                () -> sql(query)
        );

        List<String> events = readEvents(2);

        // QUERY_STARTED
        UUID queryId = verifyQueryStartedFields(events.get(0), query, null);

        verifyQueryFinishFields(events.get(1), queryId, null, query, false,
                "Failed to parse query: Encountered \\\"*\\\" at line 1, column 8");
    }

    @Test
    public void testValidationError() {
        String query = "INSERT INTO test VALUES (?), (?)";

        String expErr = "Values passed to VALUES operator must have compatible types";

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, expErr, () -> sql(query, "1", 2));

        List<String> events = readEvents(2);

        UUID queryId = verifyQueryStartedFields(events.get(0), query, null);
        verifyQueryFinishFields(events.get(1), queryId, SqlQueryType.DML, query, false, expErr);
    }

    @Test
    void testRuntimeError() {
        String query = "SELECT 1 / ?";
        String expErr = "Division by zero";

        assertThrowsSqlException(Sql.RUNTIME_ERR, expErr, () -> sql(query, 0));

        List<String> events = readEvents(2);

        UUID queryId = verifyQueryStartedFields(events.get(0), query, null);
        verifyQueryFinishFields(events.get(1), queryId, SqlQueryType.QUERY, query, true, expErr);
    }

    @Test
    void testRuntimeError2() {
        String query = "SELECT * FROM test WHERE id = (SELECT x FROM TABLE(SYSTEM_RANGE(1, 3)));";
        String expErr = "Subquery returned more than 1 value";

        assertThrowsSqlException(Sql.RUNTIME_ERR, expErr, () -> sql(query));

        List<String> events = readEvents(2);

        UUID queryId = verifyQueryStartedFields(events.get(0), query, null);
        verifyQueryFinishFields(events.get(1), queryId, SqlQueryType.QUERY, query, true, expErr);
    }

    @Test
    void testTxError() throws IOException {
        Transaction tx = igniteTx().begin();
        sql(tx, "INSERT INTO test VALUES (0)");
        tx.commit();

        readEvents(2);
        resetLog();

        String query = "INSERT INTO test VALUES (1)";;
        String expErr = "Transaction is already finished";

        assertThrowsSqlException(Transactions.TX_ALREADY_FINISHED_ERR, expErr, () -> igniteSql().execute(tx, query));

        List<String> events = readEvents(2);

        UUID queryId = verifyQueryStartedFields(events.get(0), query, tx);
        verifyQueryFinishFields(events.get(1), queryId, SqlQueryType.DML, query, tx, false, expErr, null, -1);
    }

    @Test
    void testQueryCancelledError() {
        Transaction tx = igniteTx().begin();

        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken cancellationToken = cancelHandle.token();

        String query = "SELECT x FROM TABLE(SYSTEM_RANGE(1, 100000))";

        try (ResultSet<SqlRow> ignore = igniteSql().execute(tx, cancellationToken, query)) {
            List<String> events = readEvents(1);

            UUID queryId = verifyQueryStartedFields(events.get(0), query, tx);

            cancelHandle.cancel();

            events = readEvents(2);

            verifyQueryFinishFields(events.get(1), queryId, SqlQueryType.QUERY, query, true, QueryCancelledException.CANCEL_MSG);
        }
    }

    @Test
    void testQueryTimeoutError() {
        String query = "SELECT * FROM TABLE(SYSTEM_RANGE(1, 1000000000000000))";

        Statement stmt = igniteSql().statementBuilder()
                .query(query)
                .queryTimeout(1, TimeUnit.MILLISECONDS)
                .build();

        assertThrowsSqlException(
                Sql.EXECUTION_CANCELLED_ERR,
                QueryCancelledException.TIMEOUT_MSG,
                () -> igniteSql().execute((Transaction) null, stmt)
        );

        List<String> events = readEvents(2);

        UUID queryId = verifyQueryStartedFields(events.get(0), query, null);
        verifyQueryFinishFields(events.get(1), queryId, SqlQueryType.QUERY, query, false, QueryCancelledException.TIMEOUT_MSG);
    }

    @Test
    void testDdlError() {
        String query = "CREATE TABLE test (id INT PRIMARY KEY, val INT)";
        String expErr = "Table with name 'PUBLIC.TEST' already exists";

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, expErr, () -> sql(query));

        List<String> events = readEvents(2);

        UUID queryId = verifyQueryStartedFields(events.get(0), query, null);
        verifyQueryFinishFields(events.get(1), queryId, SqlQueryType.DDL, query, false, expErr);
    }

    private static List<String> readEvents(int expectedCount) {
        return Awaitility.await().timeout(5, TimeUnit.SECONDS).until(ItSqlQueryEventLogTest::readEventLog, hasSize(expectedCount));
    }

    private static List<String> readEventLog() throws IOException {
        return Files.readAllLines(eventlogPath);
    }

    interface FieldsChecker {
        /** Checks that the specified field has expected value. */
        void verify(String field, @Nullable Object val);

        /**
         * Extracts field value using provided regular expression.
         *
         * @param field Field name.
         * @param pattern Regular expression.
         * @return Extracted field value.
         * @throws AssertionError if the specified expression is not found.
         */
        String matches(String field, String pattern);
    }

    private static UUID verifyQueryStartedFields(String event, String expectedQueryText, @Nullable Transaction tx) {
        return verifyQueryStartedFields(event, expectedQueryText, tx, null, -1);
    }

    private static UUID verifyQueryStartedFields(String event, String expectedQueryText, @Nullable Transaction tx, @Nullable UUID parentId,
            int statementNum) {
        FieldsChecker fieldsChecker = EventValidator.parseQueryStart(event);

        fieldsChecker.verify(FieldNames.INITIATOR, CLUSTER.aliveNode().name());
        fieldsChecker.verify(FieldNames.SQL, expectedQueryText);
        fieldsChecker.verify(FieldNames.SCHEMA, "PUBLIC");
        fieldsChecker.verify(FieldNames.PARENT_ID, parentId);
        fieldsChecker.verify(FieldNames.STATEMENT_NUMBER, statementNum);
        fieldsChecker.verify(FieldNames.TX_ID, tx == null ? null : ((InternalTransaction) tx).id());

        String queryIdString = fieldsChecker.matches(FieldNames.ID, UUID_PATTERN);

        return UUID.fromString(queryIdString);
    }

    private static void verifyQueryFinishFields(String event, UUID queryId, @Nullable SqlQueryType queryType, String expectedQueryText,
            boolean checkImplicitTx, @Nullable String errMessage) {
        verifyQueryFinishFields(event, queryId, queryType, expectedQueryText, null, checkImplicitTx, errMessage, null, -1);
    }

    private static void verifyQueryFinishFields(String event, UUID queryId, SqlQueryType queryType, String expectedQueryText,
            @Nullable Transaction tx, boolean checkImplicitTx, @Nullable String errMessage, @Nullable UUID parentId, int statementNum) {
        FieldsChecker fieldsChecker = EventValidator.parseQueryFinish(event);

        fieldsChecker.verify("id", queryId);
        fieldsChecker.verify(FieldNames.INITIATOR, CLUSTER.aliveNode().name());
        fieldsChecker.verify(FieldNames.SQL, expectedQueryText);
        fieldsChecker.verify(FieldNames.SCHEMA, "PUBLIC");
        fieldsChecker.verify(FieldNames.PARENT_ID, parentId);
        fieldsChecker.verify(FieldNames.STATEMENT_NUMBER, statementNum);
        fieldsChecker.verify(FieldNames.TYPE, queryType == null ? null : queryType.displayName());
        fieldsChecker.matches(FieldNames.START_TIME, "\\d+");

        if (checkImplicitTx) {
            assertThat(tx, is(nullValue()));

            fieldsChecker.matches(FieldNames.TX_ID, UUID_PATTERN);
        } else {
            fieldsChecker.verify(FieldNames.TX_ID, tx == null ? null : ((InternalTransaction) tx).id());
        }

        if (errMessage == null) {
            fieldsChecker.verify(FieldNames.ERROR, null);
        } else {
            fieldsChecker.matches(FieldNames.ERROR, ".*" + Pattern.quote(errMessage) + ".*");
        }
    }

    private static void resetLog() throws IOException {
        Files.write(eventlogPath, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static class EventValidator {
        private static final String USER = EventUser.system().username();

        private static final String PROVIDER = EventUser.system().authenticationProvider();

        private static final Pattern QUERY_START_PATTERN = Pattern.compile("\\{"
                + "\"type\":\"QUERY_STARTED\","
                + "\"timestamp\":\\d+,"
                + "\"productVersion\":\"" + IgniteProductVersion.VERSION_PATTERN.pattern() + "\","
                + "\"user\":\\{\"username\":\"" + USER + "\",\"authenticationProvider\":\"" + PROVIDER + "\"},"
                + "\"fields\":\\{(?<fields>.+)}"
                + "}");

        private static final Pattern QUERY_FINISH_PATTERN = Pattern.compile("\\{"
                + "\"type\":\"QUERY_FINISHED\","
                + "\"timestamp\":\\d+,"
                + "\"productVersion\":\"" + IgniteProductVersion.VERSION_PATTERN.pattern() + "\","
                + "\"user\":\\{\"username\":\"" + USER + "\",\"authenticationProvider\":\"" + PROVIDER + "\"},"
                + "\"fields\":\\{(?<fields>.+)}"
                + "}");

        static FieldsChecker parseQueryStart(String jsonString) {
            Matcher matcher = QUERY_START_PATTERN.matcher(jsonString);

            assertThat(matcher.matches(), is(true));

            return new FieldsCheckerImpl(matcher.group("fields"));
        }

        static FieldsChecker parseQueryFinish(String jsonString) {
            Matcher matcher = QUERY_FINISH_PATTERN.matcher(jsonString);

            assertThat("input=" + jsonString, matcher.matches(), is(true));

            return new FieldsCheckerImpl(matcher.group("fields"));
        }

        private static class FieldsCheckerImpl implements FieldsChecker {
            private final String fields;

            FieldsCheckerImpl(String fields) {
                this.fields = fields;
            }

            @Override
            public void verify(String field, Object val) {
                IgniteStringBuilder subj = new IgniteStringBuilder('"').app(field).app('"');

                subj.app(':');

                if (val == null) {
                    subj.app("null");
                } else if (val instanceof String || val instanceof UUID) {
                    subj.app('"').app(val).app('"');
                } else {
                    subj.app(val);
                }

                assertThat(fields, containsString(subj.toString()));
            }

            @Override
            public String matches(String field, String regex) {
                IgniteStringBuilder fullRegexp = new IgniteStringBuilder()
                        .app('"').app(field).app('"')
                        .app(":\"?(").app(regex).app(")\"?");

                Pattern pattern = Pattern.compile(fullRegexp.toString());

                Matcher matcher = pattern.matcher(fields);

                assertThat("pattern=" + pattern.pattern() + ", fields=" + fields, matcher.find(), is(true));

                String result = matcher.group(1);

                assertThat(result, is(notNullValue()));

                return result;
            }
        }
    }
}
