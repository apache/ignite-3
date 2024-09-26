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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base class for integration testing of key-value/record view API.
 *
 *<p>This class is used to run each test case in a consistent manner using
 * different API types (synchronous or asynchronous) and clients (embedded or thin).
 *
 * @see TestCaseType
 */
abstract class ItTableApiUnifiedBaseTest extends ClusterPerClassIntegrationTest {
    /** Default primary key columns. */
    private static final List<Column> DEF_PK = List.of(new Column("ID", NativeTypes.INT64, false));

    /** Tables to clear after each test. */
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

    void createTable(String name, List<Column> columns) {
        createTable(name, true, DEF_PK, columns);
    }

    void createTable(String name, boolean cleanup, List<Column> pkColumns, List<Column> columns) {
        String createTableTemplate = "CREATE TABLE {} ({} PRIMARY KEY ({}))";

        IgniteStringBuilder columnsBuffer = new IgniteStringBuilder();
        Set<Column> allColumns = new LinkedHashSet<>(pkColumns);

        allColumns.addAll(columns);

        for (Column column : allColumns) {
            RelDataType sqlType = TypeUtils.native2relationalType(Commons.typeFactory(), column.type());

            String sqlTypeString = sqlType.toString();

            // TODO remove after https://issues.apache.org/jira/browse/IGNITE-23130
            if (SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE == sqlType.getSqlTypeName()) {
                sqlTypeString = "TIMESTAMP(" + sqlType.getPrecision() + ") WITH LOCAL TIME ZONE";
            }

            columnsBuffer.app(column.name()).app(' ').app(sqlTypeString);

            if (!column.nullable()) {
                columnsBuffer.app(" NOT NULL");
            }

            columnsBuffer.app(", ");
        }

        String pkColumnNames = pkColumns.stream().map(Column::name).collect(Collectors.joining(", "));

        String query = IgniteStringFormatter.format(createTableTemplate, name, columnsBuffer, pkColumnNames);

        sql(query);

        if (cleanup) {
            tablesToClear.add(name);
        }
    }

    protected List<List<Object>> sql(String sql) {
        log.info(">sql> " + sql);

        return sql(null, sql, ArrayUtils.OBJECT_EMPTY_ARRAY);
    }

    static void assertEqualsValues(SchemaDescriptor schema, Tuple expected, @Nullable Tuple actual) {
        assertNotNull(actual);

        for (int i = 0; i < schema.valueColumns().size(); i++) {
            Column col = schema.valueColumns().get(i);

            Object val1 = expected.value(col.name());
            Object val2 = actual.value(col.name());

            if (val1 instanceof byte[] && val2 instanceof byte[]) {
                Assertions.assertArrayEquals((byte[]) val1, (byte[]) val2, "Equality check failed: colIdx=" + col.positionInRow());
            } else {
                assertEquals(val1, val2, "Equality check failed: colIdx=" + col.positionInRow());
            }
        }
    }

    private static List<String> getClientAddresses(List<Ignite> nodes) {
        return nodes.stream()
                .map(ignite -> unwrapIgniteImpl(ignite).clientAddress().port())
                .map(port -> "127.0.0.1" + ":" + port)
                .collect(toList());
    }

    /**
     * Defines the type of test case to run.
     */
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
