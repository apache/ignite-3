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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base class for integration testing of views API.
 */
abstract class ItViewsApiUnifiedTest extends ClusterPerClassIntegrationTest {
    private final List<String> tablesToClear = new ArrayList<>();

    protected IgniteClient client;

    @BeforeAll
    public void initialize() {
        client = IgniteClient.builder()
                .addresses(getClientAddresses(List.of(CLUSTER.aliveNode())).get(0))
                .build();
    }

    @AfterEach
    public void clearData() {
        for (String tableName : tablesToClear) {
            sql("DELETE FROM " + tableName);
        }
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    void createTable(String name, Column... columns) {
        createTable(name, true, columns);
    }

    void createTable(String name, boolean clear, Column... columns) {
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

            if (!column.nullable()) {
                buffer.app(" NOT NULL");
            }
        }

        String query = IgniteStringFormatter.format(createTableTemplate, name, buffer);

        log.info(">sql> " + query);

        sql(query);

        if (clear) {
            tablesToClear.add(name);
        }
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
}
