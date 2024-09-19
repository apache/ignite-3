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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for key-value API tests.
 */
public abstract class KvApiBaseTest extends ClusterPerClassIntegrationTest {
    static final String TABLE_NAME_COMPLEX_TYPE = "test";

    static final String TABLE_NAME_SIMPLE_TYPE = "test_simple";

    protected IgniteClient client;

    @BeforeAll
    public void initialize() {
        client = IgniteClient.builder()
                .addresses(getClientAddresses(List.of(CLUSTER.aliveNode())).get(0))
                .build();

        IgniteStringBuilder buffer = new IgniteStringBuilder();

        for (Column column : KeyValueTestUtils.ALL_TYPES_COLUMNS) {
            RelDataType sqlType = TypeUtils.native2relationalType(Commons.typeFactory(), column.type());

            String sqlTypeString = sqlType.toString();

            // TODO remove after https://issues.apache.org/jira/browse/IGNITE-23130
            if (SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE == sqlType.getSqlTypeName()) {
                sqlTypeString = "TIMESTAMP(" + sqlType.getPrecision() + ") WITH LOCAL TIME ZONE";
            }

            buffer.app(", ").app(column.name()).app(' ').app(sqlTypeString);
        }

        String createTableTemplate = "CREATE TABLE {} (id BIGINT PRIMARY KEY{})";

        String query = IgniteStringFormatter.format(createTableTemplate, TABLE_NAME_COMPLEX_TYPE, buffer);
        log.info(">sql> " + query);
        sql(query);

        query = IgniteStringFormatter.format(createTableTemplate, TABLE_NAME_SIMPLE_TYPE, ", VAL VARCHAR");
        log.info(">sql> " + query);
        sql(query);
    }

    @BeforeEach
    public void clearData() {
        sql("DELETE FROM test");
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    /**
     * Gets client connector addresses for the specified nodes.
     *
     * @param nodes Nodes.
     * @return List of client addresses.
     */
    private static List<String> getClientAddresses(List<Ignite> nodes) {
        return nodes.stream()
                .map(ignite -> unwrapIgniteImpl(ignite).clientAddress().port())
                .map(port -> "127.0.0.1" + ":" + port)
                .collect(toList());
    }
}
