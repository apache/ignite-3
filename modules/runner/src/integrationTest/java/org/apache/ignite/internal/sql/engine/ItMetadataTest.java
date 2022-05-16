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

package org.apache.ignite.internal.sql.engine;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.generate;

import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Group of tests to verify the query metadata returned alongside the query result.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ItMetadataTest extends AbstractBasicIntegrationTest {
    /**
     * Before all.
     */
    @BeforeAll
    static void initTestData() {
        createAndPopulateTable();
    }

    @Test
    public void trimColumnNames() {
        String var300 = generate(() -> "X").limit(300).collect(joining());
        String var256 = "'" + var300.substring(0, 255);

        assertQuery("select '" + var300 + "' from person").columnNames(var256).check();
    }

    @Test
    public void columnNames() {
        assertQuery("select (select count(*) from person), (select avg(salary) from person) from person")
                .columnNames("EXPR$0", "EXPR$1").check();
        assertQuery("select (select count(*) from person) as subquery from person")
                .columnNames("SUBQUERY").check();

        assertQuery("select salary*2, salary/2, salary+2, salary-2, mod(salary, 2)  from person")
                .columnNames("SALARY * 2", "SALARY / 2", "SALARY + 2", "SALARY - 2", "MOD(SALARY, 2)").check();
        assertQuery("select salary*2 as first, salary/2 as secOND from person").columnNames("FIRST", "SECOND").check();

        assertQuery("select trim(name) tr_name from person").columnNames("TR_NAME").check();
        assertQuery("select trim(name) from person").columnNames("TRIM(BOTH ' ' FROM NAME)").check();
        assertQuery("select ceil(salary), floor(salary), position('text' IN salary) from person")
                .columnNames("CEIL(SALARY)", "FLOOR(SALARY)", "POSITION('text' IN SALARY)").check();

        assertQuery("select count(*) from person").columnNames("COUNT(*)").check();
        assertQuery("select count(name) from person").columnNames("COUNT(NAME)").check();
        assertQuery("select max(salary) from person").columnNames("MAX(SALARY)").check();
        assertQuery("select min(salary) from person").columnNames("MIN(SALARY)").check();
        assertQuery("select aVg(salary) from person").columnNames("AVG(SALARY)").check();
        assertQuery("select sum(salary) from person").columnNames("SUM(SALARY)").check();

        assertQuery("select salary, count(name) from person group by salary").columnNames("SALARY", "COUNT(NAME)").check();

        assertQuery("select 1, -1, 'some string' from person").columnNames("1", "-1", "'some string'").check();
    }

    @Test
    public void infixTypeCast() {
        assertQuery("select id, id::tinyint as tid, id::smallint as sid, id::varchar as vid, id::interval hour, "
                + "id::interval year from person")
                .columnNames("ID", "TID", "SID", "VID", "ID :: INTERVAL INTERVAL_HOUR", "ID :: INTERVAL INTERVAL_YEAR")
                // TODO: IGNITE-16635 replace byte arrays for correct types.
                //.columnTypes(Integer.class, Byte.class, Short.class, String.class, Duration.class, Period.class)
                .columnTypes(Integer.class, Byte.class, Short.class, String.class, byte[].class, byte[].class)
                .check();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16679")
    public void columnOrder() {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "COLUMN_ORDER").columns(
                SchemaBuilders.column("DOUBLE_C", ColumnType.DOUBLE).asNullable(true).build(),
                SchemaBuilders.column("LONG_C", ColumnType.INT64).build(),
                SchemaBuilders.column("STRING_C", ColumnType.string()).asNullable(true).build(),
                SchemaBuilders.column("INT_C", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("LONG_C").build();

        CLUSTER_NODES.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(schTbl1, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        );

        assertQuery("select * from column_order")
                .columnNames("DOUBLE_C", "LONG_C", "STRING_C", "INT_C")
                .check();
    }
}
