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

package org.apache.ignite.internal.sql.engine.events;

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.eventlog.api.IgniteEvents;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.tx.InternalTransaction;
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
 * Integration tests to check the {@link IgniteEvents#QUERY_STARTED} and {@link IgniteEvents#QUERY_FINISHED} events.
 */
public class ItSqQueryEventLogTest extends BaseSqlIntegrationTest {
    private static Path eventlogPath;

    @BeforeAll
    static void captureEventLogPath() {
        String buildDirPath = System.getProperty("user.dir") + "/build/";
        eventlogPath = Path.of(buildDirPath).resolve("event.log");

        sql("CREATE TABLE test(id INT PRIMARY KEY)");
    }

    @BeforeEach
    void tearDown() throws IOException {
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
    void testSimpleQuery() {
        String query = "SELECT 1";
        sql(query);

        List<String> events = readEvents(2);

        UUID queryId = verifyQueryStartedFields(events.get(0), query, null);
        verifyQueryFinishFields(events.get(1), queryId, null);
    }

    @Test
    void testMultiStatementQuery() {
        String scriptText = "INSERT INTO test VALUES (0); SELECT 1;";
        igniteSql().executeScript(scriptText);

        List<String> events = readEvents(6);

        UUID scriptId = verifyQueryStartedFields(events.get(0), scriptText, null);

        UUID insertQueryId = verifyQueryStartedFields(events.get(1), "INSERT INTO `TEST`\\nVALUES ROW(0)", null, scriptId, 0);
        verifyQueryFinishFields(events.get(2), insertQueryId, null);

        UUID selectQueryId = verifyQueryStartedFields(events.get(3), "SELECT 1", null, scriptId, 1);

        // The order in which the last `SELECT` statement finish and script statement finish events are sent is undefined.
        String eventCompositeEvent = events.get(4) + events.get(5);
        verifyQueryFinishFields(eventCompositeEvent, selectQueryId, null);
        verifyQueryFinishFields(eventCompositeEvent, scriptId, null);
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

        verifyQueryFinishFields(events.get(1), queryId, "Failed to parse query: Encountered \\\"*\\\" at line 1, column 8");
    }

    @Test
    public void testValidationError() {
        String query = "INSERT INTO test VALUES (?), (?)";

        String expErr = "Values passed to VALUES operator must have compatible types";

        assertThrowsSqlException(STMT_VALIDATION_ERR, expErr, () -> sql(query, "1", 2));

        List<String> events = readEvents(2);

        UUID queryId = verifyQueryStartedFields(events.get(0), query, null);

        verifyQueryFinishFields(events.get(1), queryId, expErr);
    }

    @Test
    void testRuntimeError() {
        String query = "SELECT 1 / ?";
        String expErr = "Division by zero";

        assertThrowsSqlException(Sql.RUNTIME_ERR, expErr, () -> sql(query, 0));

        List<String> events = readEvents(2);

        UUID queryId = verifyQueryStartedFields(events.get(0), query, null);
        verifyQueryFinishFields(events.get(1), queryId, expErr);
    }

    @Test
    void testRuntimeError2() {
        String query = "SELECT * FROM test WHERE id = (SELECT x FROM TABLE(SYSTEM_RANGE(1, 3)));";
        String expErr = "Subquery returned more than 1 value";

        assertThrowsSqlException(Sql.RUNTIME_ERR, expErr, () -> sql(query));

        List<String> events = readEvents(2);

        UUID queryId = verifyQueryStartedFields(events.get(0), query, null);
        verifyQueryFinishFields(events.get(1), queryId, expErr);
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
        verifyQueryFinishFields(events.get(1), queryId, expErr);
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

            verifyQueryFinishFields(events.get(1), queryId, QueryCancelledException.CANCEL_MSG);
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
                () -> igniteSql().execute(null, stmt)
        );

        List<String> events = readEvents(2);

        UUID queryId = verifyQueryStartedFields(events.get(0), query, null);
        verifyQueryFinishFields(events.get(1), queryId, QueryCancelledException.TIMEOUT_MSG);
    }

    private static List<String> readEvents(int expectedCount) {
        return Awaitility.await().timeout(5, TimeUnit.SECONDS).until(ItSqQueryEventLogTest::readEventLog, hasSize(expectedCount));
    }

    private static List<String> readEventLog() throws IOException {
        return Files.readAllLines(eventlogPath);
    }

    interface FieldsChecker {
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

        fieldsChecker.verify("initiator", CLUSTER.aliveNode().name());
        fieldsChecker.verify("sql", expectedQueryText);
        fieldsChecker.verify("schema", "PUBLIC");
        fieldsChecker.verify("parentId", parentId);
        fieldsChecker.verify("statementNum", statementNum);
        fieldsChecker.verify("transactionId", tx == null ? null : ((InternalTransaction) tx).id());

        String queryIdString = fieldsChecker.matches("id", "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

        return UUID.fromString(queryIdString);
    }

    private static void verifyQueryFinishFields(String event, UUID queryId, @Nullable String errMessage) {
        FieldsChecker fieldsChecker = EventValidator.parseQueryFinish(event);

        fieldsChecker.verify("id", queryId);

        if (errMessage == null) {
            fieldsChecker.verify("error", null);
        } else {
            fieldsChecker.matches("error", ".*" + Pattern.quote(errMessage) + ".*");
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
                + "\"timestamp\":\\d*,"
                + "\"productVersion\":\"" + IgniteProductVersion.VERSION_PATTERN.pattern() + "\","
                + "\"user\":\\{\"username\":\"" + USER + "\",\"authenticationProvider\":\"" + PROVIDER + "\"},"
                + "\"fields\":\\{(?<fields>.+)}"
                + "}");

        private static final Pattern QUERY_FINISH_PATTERN = Pattern.compile("\\{"
                + "\"type\":\"QUERY_FINISHED\","
                + "\"timestamp\":\\d*,"
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

            assertThat(matcher.matches(), is(true));

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
                        .app(":\"(").app(regex).app(")\"");

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
