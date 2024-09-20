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

package org.apache.ignite.internal.table;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Base class for key-value API integration tests.
 */
public abstract class ItKeyValueViewApiBaseTest extends ClusterPerClassIntegrationTest {
    protected IgniteClient client;

    private final List<String> tables = new ArrayList<>();

    @BeforeAll
    public void initialize() {
        client = IgniteClient.builder()
                .addresses(getClientAddresses(List.of(CLUSTER.aliveNode())).get(0))
                .build();
    }

    @AfterEach
    public void clearData() {
        for (String tableName : tables) {
            sql("DELETE FROM " + tableName);
        }
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    abstract TestCaseFactory getFactory(String tableName);

    void createTable(String name, Column... columns) {
        String createTableTemplate = "CREATE TABLE {} (id BIGINT PRIMARY KEY{})";

        IgniteStringBuilder buffer = new IgniteStringBuilder();

        for (Column column : columns) {
            RelDataType sqlType = TypeUtils.native2relationalType(Commons.typeFactory(), column.type());

            String sqlTypeString = sqlType.toString();

            // TODO remove after https://issues.apache.org/jira/browse/IGNITE-23130
            if (SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE == sqlType.getSqlTypeName()) {
                sqlTypeString = "TIMESTAMP(" + sqlType.getPrecision() + ") WITH LOCAL TIME ZONE";
            }

            buffer.app(", ").app(column.name()).app(' ').app(sqlTypeString);
        }

        String query = IgniteStringFormatter.format(createTableTemplate, name, buffer);

        log.info(">sql> " + query);

        sql(query);

        tables.add(name);
    }

    private static List<String> getClientAddresses(List<Ignite> nodes) {
        return nodes.stream()
                .map(ignite -> unwrapIgniteImpl(ignite).clientAddress().port())
                .map(port -> "127.0.0.1" + ":" + port)
                .collect(toList());
    }

    enum TestCaseType {
        EMBEDDED("embedded"),
        EMBEDDED_ASYNC("embedded async"),
        THIN("thin"),
        THIN_ASYNC("thin async");

        private final String description;

        TestCaseType(String description) {
            this.description = description;
        }

        String description() {
            return description;
        }
    }

    abstract static class TestCaseFactory {
        final String tableName;

        TestCaseFactory(String tableName) {
            this.tableName = tableName;
        }

        <K, V> BaseTestCase<K, V> create(TestCaseType type, Class<K> keyClass, Class<V> valueClass) {
            switch (type) {
                case EMBEDDED:
                    return create(false, false, keyClass, valueClass);
                case EMBEDDED_ASYNC:
                    return create(true, false, keyClass, valueClass);
                case THIN:
                    return create(false, true, keyClass, valueClass);
                case THIN_ASYNC:
                    return create(true, true, keyClass, valueClass);
                default:
                    throw new IllegalArgumentException("Unknown test case type: " + type);
            }
        }

        abstract <K, V> BaseTestCase<K, V> create(boolean async, boolean thin, Class<K> keyClass, Class<V> valueClass);
    }

    List<Arguments> generateKeyValueTestArguments(String tableName, Class<?> keyClass, Class<?> valueClass) {
        TestCaseFactory caseFactory = getFactory(tableName);

        List<Arguments> arguments = new ArrayList<>(TestCaseType.values().length);

        for (TestCaseType type : TestCaseType.values()) {
            arguments.add(Arguments.of(Named.of(
                    type.description(),
                    caseFactory.create(type, keyClass, valueClass)
            )));
        }

        return arguments;
    }

    static class BaseTestCase<K, V> {
        final boolean async;
        final boolean thin;
        final KeyValueView<K, V> view;

        BaseTestCase(boolean async, boolean thin, KeyValueView<K, V> view) {
            this.async = async;
            this.thin = thin;
            this.view = view;
        }

        KeyValueView<K, V> view() {
            return view;
        }

        void checkNullKeyError(Executable run) {
            checkNpeMessage(run, "key");
        }

        void checkNullKeysError(Executable run) {
            checkNpeMessage(run, "keys");
        }

        void checkNullPairsError(Executable run) {
            checkNpeMessage(run, "pairs");
        }

        @SuppressWarnings("ThrowableNotThrown")
        static void checkNpeMessage(Executable run, String message) {
            IgniteTestUtils.assertThrows(NullPointerException.class, run, message);
        }
    }
}
