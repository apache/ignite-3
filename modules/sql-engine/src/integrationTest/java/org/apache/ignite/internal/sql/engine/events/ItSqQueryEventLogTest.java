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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ItSqQueryEventLogTest extends BaseSqlIntegrationTest {
    private static Path eventlogPath;

    @BeforeAll
    static void captureEventLogPath() {
        String buildDirPath = System.getProperty("buildDirPath");
        eventlogPath = Path.of(buildDirPath).resolve("event.log");
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
                + " [QUERY_STARTED, QUERY_FINISHED],\n"
                + "}\n";

        builder.clusterConfiguration("ignite {\n" + eventLog + "}");
    }

    @Test
    void simpleQuery() throws IOException {
        assertThat(readEventLog(), is(empty()));

        sql("SELECT 1");

        List<String> events = await().until(ItSqQueryEventLogTest::readEventLog, hasSize(2));

        String id;

        // QUERY_STARTED
        {
            FieldsChecker checker = EventValidator.parseQueryStart(events.get(0));

            checker.verify("initiator", CLUSTER.aliveNode().name());
            checker.verify("sql", "SELECT 1");
            checker.verify("schema", "PUBLIC");
            checker.verify("statementNum", -1);
            checker.verify("transactionId", null);
            checker.verify("parentId", null);

            id = checker.extract("id", "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
        }

        // QUERY_FINISHED
        {
            FieldsChecker checker = EventValidator.parseQueryFinish(events.get(1));

            checker.verify("id", id);
            checker.verify("error", null);
        }
    }

    interface FieldsChecker {
        void verify(String field, @Nullable Object val);

        String extract(String field, String pattern);
    }

    static class EventValidator {
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
            public String extract(String field, String regex) {
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

    private static List<String> readEventLog() throws IOException {
        return Files.readAllLines(eventlogPath);
    }
}
