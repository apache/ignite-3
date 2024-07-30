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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.prepare.ParameterType;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.junit.jupiter.params.provider.MethodSource;

/** Dynamic parameters checks. */
public class ItDynamicParameterTest extends BaseSqlIntegrationTest {

    @BeforeEach
    public void createTable() {
        sql("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val1 INTEGER NOT NULL, val2 INTEGER)");
    }

    @AfterEach
    public void dropTables() {
        sql("DROP TABLE IF EXISTS t1");
    }

    @ParameterizedTest
    @EnumSource(value = ColumnType.class,
            //    https://issues.apache.org/jira/browse/IGNITE-15200
            names = {"DURATION", "PERIOD"},
            mode = Mode.EXCLUDE
    )
    void testMetadataTypesForDynamicParameters(ColumnType type) {
        Object param = SqlTestUtils.generateValueByType(type);
        List<List<Object>> ret = sql("SELECT typeof(?)", param);
        String type0 = (String) ret.get(0).get(0);

        assertTrue(type0.startsWith(SqlTestUtils.toSqlType(type)));
        assertQuery("SELECT ?").withParams(param).returns(param).columnMetadata(new MetadataMatcher().type(type)).check();
    }

    @Test
    public void testDynamicParameters() {
        assertQuery("SELECT COALESCE(null, ?)").withParams(13).returns(13).check();
        assertQuery("SELECT LOWER(?)").withParams("ASD").returns("asd").check();
        assertQuery("SELECT LOWER(?)").withParams(null).returns(null).check();
        assertQuery("SELECT POWER(?, ?)").withParams(2, 3).returns(8d).check();
        assertQuery("SELECT SQRT(?)").withParams(4d).returns(2d).check();
        assertQuery("SELECT ?").withParams("asd").returns("asd").check();
        assertQuery("SELECT ? % ?").withParams(11, 10).returns(1).check();
        assertQuery("SELECT ? + ?, LOWER(?) ").withParams(2, 2, "TeSt").returns(4, "test").check();
        assertQuery("SELECT LOWER(?), ? + ? ").withParams("TeSt", 2, 2).returns("test", 4).check();
        assertQuery("SELECT (? + 1)::INTEGER").withParams(1).returns(2).check();

        createAndPopulateTable();
        assertQuery("SELECT name LIKE '%' || ? || '%' FROM person where name is not null").withParams("go").returns(true).returns(false)
                .returns(false).returns(false).check();

        assertQuery("SELECT id FROM person WHERE name LIKE ? ORDER BY id LIMIT ?").withParams("I%", 1).returns(0).check();
        assertQuery("SELECT id FROM person WHERE name LIKE ? ORDER BY id LIMIT ? OFFSET ?").withParams("I%", 1, 1).returns(2).check();
        assertQuery("SELECT id from person WHERE salary<? and id<?").withParams(15, 3).returns(0).check();

        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Unsupported dynamic parameter defined",
                () -> sql("SELECT LAST_DAY(?)", Date.valueOf("2022-01-01")));

        LocalDate date1 = LocalDate.parse("2022-01-01");
        LocalDate date2 = LocalDate.parse("2022-01-31");

        assertQuery("SELECT LAST_DAY(?)").withParams(date1).returns(date2).check();
    }

    /**
     * Tests a nested CASE WHEN statement using various combinations of dynamic parameter values,
     * including the {@code NULL} value for different operands.
     */
    @Test
    public void testNestedCase() {
        sql("CREATE TABLE TBL1(ID INT PRIMARY KEY, VAL VARCHAR, NUM INT)");

        assertTrue(sql(
                "select case when (_T0.VAL is not distinct from ?) then 0 else (case when (_T0.VAL > ?) then 1 else -1 end) end "
                        + "from PUBLIC.TBL1 as _T0", "abc", "abc").isEmpty());

        sql("INSERT INTO TBL1 VALUES"
                + " (0, 'abc', 0),"
                + " (1, 'abc', NULL),"
                + " (2, NULL, 0)");

        String sql = "select case when (VAL = ?) then 0 else (case when (NUM = ?) then ? else ? end) end ";
        assertQuery(sql + "from TBL1").withParams("abc", 0, 1, null).returns(0).returns(0).returns(1).check();
        assertQuery(sql + "IS NULL from TBL1").withParams("abc", 0, 1, null).returns(false).returns(false).returns(false).check();

        sql = "select case when (VAL = ?) then 0 else (case when (NUM IS NULL) then ? else ? end) end ";
        // NULL in THEN operand.
        assertQuery(sql + "from TBL1").withParams("diff", null, 1).returns(1).returns((Object) null).returns(1).check();
        assertQuery(sql + "IS NULL from TBL1").withParams("diff", null, 1).returns(false).returns(true).returns(false).check();
        // NULL in ELSE operand.
        assertQuery(sql + "from TBL1").withParams("diff", 1, null).returns((Object) null).returns(1).returns((Object) null).check();
        assertQuery(sql + "IS NULL from TBL1").withParams("diff", 1, null).returns(true).returns(false).returns(true).check();

        sql = "select case when (VAL is not distinct from ?) then '0' else (case when (NUM is not distinct from ?) then ? else ? end) end ";
        assertQuery(sql + "from TBL1").withParams(null, null, "1", null).returns((Object) null).returns("1").returns("0").check();
        assertQuery(sql + "IS NULL from TBL1").withParams(null, null, "1", null).returns(true).returns(false).returns(false).check();
    }

    /** Need to test the same query with different type of parameters to cover case with check right plans cache work. **/
    @Test
    public void testWithDifferentParametersTypes() {
        assertQuery("SELECT ? + ?, LOWER(?) ").withParams(2, 2, "TeSt").returns(4, "test").check();
        assertQuery("SELECT ? + ?, LOWER(?) ").withParams(2.2, 2.2, "TeSt").returns(4.4, "test").check();

        assertQuery("SELECT COALESCE(?, ?)").withParams(null, null).returns(null).check();
        assertQuery("SELECT COALESCE(?, ?)").withParams(null, 13).returns(13).check();
        assertQuery("SELECT COALESCE(?, ?)").withParams("a", "b").returns("a").check();
        assertQuery("SELECT COALESCE(?, ?)").withParams(22, 33).returns(22).check();

        assertQuery("SELECT UPPER(TYPEOF(?))").withParams(1).returns("INTEGER").check();
        assertQuery("SELECT UPPER(TYPEOF(?))").withParams(1d).returns("DOUBLE").check();

        assertQuery("SELECT ?::INTEGER = '8'").withParams(8).returns(true).check();
    }

    /**
     * SQL 2016, clause 9.5: Mixing types in CASE/COALESCE expressions is illegal.
     */
    @Test
    public void testWithDifferentParametersTypesMismatch() {
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Illegal mixing of types in CASE or COALESCE statement",
                () -> assertQuery("SELECT COALESCE(12.2, ?)").withParams("b").check());
        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR,
                "Illegal mixing of types in CASE or COALESCE statement",
                () -> assertQuery("SELECT COALESCE(?, ?)").withParams(12.2, "b").check());
    }

    @Test
    public void testUnspecifiedDynamicParameterInExplain() {
        assertUnexpectedNumberOfParameters("EXPLAIN PLAN FOR SELECT * FROM t1 WHERE id > ?");
    }

    @Test
    public void testNullExprs() {
        Object[] rowNull = {null};

        assertQuery("SELECT 1 + NULL").returns(rowNull).check();
        assertQuery("SELECT 1 * NULL").returns(rowNull).check();

        assertQuery("SELECT 1 + ?").withParams(rowNull).returns(rowNull).check();
        assertQuery("SELECT ? + 1").withParams(rowNull).returns(rowNull).check();

        assertQuery("SELECT ? + ?").withParams(1, null).returns(rowNull).check();
        assertQuery("SELECT ? + ?").withParams(null, 1).returns(rowNull).check();
        assertQuery("SELECT ? + ?").withParams(null, null).returns(rowNull).check();
    }

    @Test
    public void testDynamicParametersInExplain() {
        sql("EXPLAIN PLAN FOR SELECT * FROM t1 WHERE id > ?", 1);
    }

    @Test
    public void testUnspecifiedDynamicParameterInSelectList() {
        assertUnexpectedNumberOfParameters("SELECT COALESCE(?)");
        assertUnexpectedNumberOfParameters("SELECT * FROM (VALUES(1, 2, ?)) t1");
    }

    @Test
    public void testUnspecifiedDynamicParameterInInsert() {
        assertUnexpectedNumberOfParameters("INSERT INTO t1 VALUES(1, 2, ?)");
    }

    @Test
    public void testUnspecifiedDynamicParameterInUpdate() {
        // column value
        assertUnexpectedNumberOfParameters("UPDATE t1 SET val1=? WHERE id = 1");
        // predicate
        assertUnexpectedNumberOfParameters("UPDATE t1 SET val1=10 WHERE id = ?");
    }

    @Test
    public void testUnspecifiedDynamicParameterInDelete() {
        assertUnexpectedNumberOfParameters("DELETE FROM t1 WHERE id = ? AND val1=1");
    }

    @Test
    public void testUnexpectedNumberOfParametersInSelectList() {
        assertUnexpectedNumberOfParameters("SELECT 1", 1);
        assertUnexpectedNumberOfParameters("SELECT ?", 1, 2);
    }

    @Test
    public void testUnexpectedNumberOfParametersInSelectInInsert() {
        assertUnexpectedNumberOfParameters("INSERT INTO t1 VALUES(1, 2, 3)", 1);
        assertUnexpectedNumberOfParameters("INSERT INTO t1 VALUES(1, 2, ?)", 1, 2);
    }

    @Test
    public void testUnexpectedNumberOfParametersInDelete() {
        assertUnexpectedNumberOfParameters("DELETE FROM t1 WHERE id = 1 AND val1=1", 1);
        assertUnexpectedNumberOfParameters("DELETE FROM t1 WHERE id = ? AND val1=1", 1, 2);
    }

    @Test
    public void testUnspecifiedDynamicParameterInLimitOffset() {
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT 1", 1);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT ?", 1, 2);

        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT 1 OFFSET 1", 1);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT 1 OFFSET ?", 1, 2);

        assertUnexpectedNumberOfParameters("SELECT * FROM t1 OFFSET 1", 1);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 OFFSET ?", 1, 2);
    }

    /** varchar casts - literals. */
    @ParameterizedTest
    @MethodSource("varcharCasts")
    public void testVarcharCastsLiterals(String value, RelDataType type, String result) {
        String query = format("SELECT CAST('{}' AS {})", value, type);
        assertQuery(query).returns(result).check();
    }

    /** varchar casts - dynamic params. */
    @ParameterizedTest
    @MethodSource("varcharCasts")
    public void testVarcharCastsDynamicParams(String value, RelDataType type, String result) {
        String query = format("SELECT CAST(? AS {})", type);
        assertQuery(query).withParams(value).returns(result).check();
    }

    private static Stream<Arguments> varcharCasts() {
        IgniteTypeFactory typeFactory = Commons.typeFactory();

        return Stream.of(
                // varchar
                arguments("abcde", typeFactory.createSqlType(SqlTypeName.VARCHAR, 3), "abc"),
                arguments("abcde", typeFactory.createSqlType(SqlTypeName.VARCHAR, 5), "abcde"),
                arguments("abcde", typeFactory.createSqlType(SqlTypeName.VARCHAR, 6), "abcde"),
                arguments("abcde", typeFactory.createSqlType(SqlTypeName.VARCHAR), "abcde"),

                // char
                arguments("abcde", typeFactory.createSqlType(SqlTypeName.CHAR), "a"),
                arguments("abcde", typeFactory.createSqlType(SqlTypeName.CHAR, 3), "abc")
        );
    }

    @ParameterizedTest
    @MethodSource("statementsWithParameters")
    public void testGetParameterTypesSimple(String stmt, List<ColumnType> expectedTypes, Object[] params) {
        List<ParameterType> parameterTypes = getParameterTypes(stmt, params);

        List<ColumnType> columnTypes = parameterTypes
                .stream()
                .map(ParameterType::columnType)
                .collect(Collectors.toList());

        assertEquals(expectedTypes, columnTypes, stmt);
    }

    private static Stream<Arguments> statementsWithParameters() {
        return Stream.of(
                arguments("SELECT CAST(? AS BIGINT)", List.of(ColumnType.INT64), new Object[0]),
                arguments("SELECT CAST(? AS BIGINT)", List.of(ColumnType.INT32), new Object[]{1}),
                arguments("SELECT CAST(? AS BIGINT)", List.of(ColumnType.STRING), new Object[]{"1"}),

                arguments("INSERT INTO t1 VALUES(1, ?, ?)", List.of(ColumnType.INT32, ColumnType.INT32), new Object[0]),
                arguments("UPDATE t1 SET val1 = ? WHERE id = ?", List.of(ColumnType.INT32, ColumnType.INT32), new Object[0]),
                arguments("SELECT val1 + ? FROM t1 WHERE id = ?", List.of(ColumnType.INT32, ColumnType.INT32), new Object[0])
        );
    }

    @Test
    public void testRejectPrepareWithMoreParameters() {
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Unexpected number of query parameters",
                () -> getParameterTypes("SELECT ? + ?", 1, 2, 3));
    }

    @Test
    public void testGetUnspecifiedParameterTypesInPredicate() {
        List<Pair<String, NativeType>> dataTypes = columnNameAndType();

        sql(createTableFoColumns("params0", dataTypes));

        StringBuilder stmt = new StringBuilder("SELECT * FROM params0 WHERE ");
        for (int i = 0; i < dataTypes.size(); i++) {
            if (i > 0) {
                stmt.append(" AND ");
            }
            stmt.append(dataTypes.get(i).getFirst());
            stmt.append(" = ?");
        }

        log.info("SELECT from column names: {}", stmt);

        List<ParameterType> parameterTypes = getParameterTypes(stmt.toString());

        List<NativeType> actualTypes = parameterTypes.stream()
                .map(p -> TypeUtils.columnType2NativeType(p.columnType(), p.precision(), p.scale(), p.precision()))
                .collect(Collectors.toList());

        assertEquals(actualTypes, dataTypes.stream().map(Pair::getSecond).collect(Collectors.toList()), "parameter types");
    }

    @Test
    public void testGetUnspecifiedParameterTypesInInsert() {
        List<Pair<String, NativeType>> dataTypes = columnNameAndType();

        sql(createTableFoColumns("params1", dataTypes));

        StringBuilder stmt = new StringBuilder("INSERT INTO params1 VALUES(1");
        stmt.append(", ?".repeat(dataTypes.size()));
        stmt.append(")");

        log.info("INSERT from column names: {}", stmt);

        List<ParameterType> parameterTypes = getParameterTypes(stmt.toString());

        List<NativeType> actualTypes = parameterTypes.stream()
                .map(p -> TypeUtils.columnType2NativeType(p.columnType(), p.precision(), p.scale(), p.precision()))
                .collect(Collectors.toList());

        assertEquals(actualTypes, dataTypes.stream().map(Pair::getSecond).collect(Collectors.toList()), "parameter types");
    }

    @Test
    public void testGetUnspecifiedParameterTypesInUpdate() {
        List<Pair<String, NativeType>> dataTypes = columnNameAndType();

        sql(createTableFoColumns("params2", dataTypes));

        StringBuilder stmt = new StringBuilder("UPDATE params2 SET ");
        for (int i = 0; i < dataTypes.size(); i++) {
            if (i > 0) {
                stmt.append(", ");
            }
            String colName = dataTypes.get(i).getFirst();
            stmt.append(colName);
            stmt.append("=?");
        }

        log.info("UPDATE from column names: {}", stmt);

        List<ParameterType> parameterTypes = getParameterTypes(stmt.toString());

        List<NativeType> actualTypes = parameterTypes.stream()
                .map(p -> TypeUtils.columnType2NativeType(p.columnType(), p.precision(), p.scale(), p.precision()))
                .collect(Collectors.toList());

        assertEquals(actualTypes, dataTypes.stream().map(Pair::getSecond).collect(Collectors.toList()), "parameter types");
    }

    private static List<Pair<String, NativeType>> columnNameAndType() {
        return Arrays.asList(
                new Pair<>("bool_col", NativeTypes.BOOLEAN),
                new Pair<>("int8_col", NativeTypes.INT8),
                new Pair<>("int16_col", NativeTypes.INT16),
                new Pair<>("int32_col", NativeTypes.INT32),
                new Pair<>("int64_col", NativeTypes.INT64),
                new Pair<>("float_col", NativeTypes.FLOAT),
                new Pair<>("double_col", NativeTypes.DOUBLE),
                new Pair<>("dec_col", NativeTypes.decimalOf(5, 2)),
                new Pair<>("date_col", NativeTypes.DATE),
                new Pair<>("datetime_col", NativeTypes.datetime(2)),
                new Pair<>("time_col", NativeTypes.time(2)),
                new Pair<>("string_col", NativeTypes.stringOf(10)),
                new Pair<>("bytes_col", NativeTypes.blobOf(10)),
                new Pair<>("uuid_col", NativeTypes.UUID)
        );
    }

    private String createTableFoColumns(String name, List<Pair<String, NativeType>> dataTypes) {
        IgniteTypeFactory tf = Commons.typeFactory();

        StringBuilder ddl = new StringBuilder("CREATE TABLE ");
        ddl.append(name);
        ddl.append("(id INTEGER PRIMARY KEY");

        for (Pair<String, NativeType> entry : dataTypes) {
            RelDataType relDataType = TypeUtils.native2relationalType(tf, entry.getSecond(), true);

            ddl.append(", ");
            ddl.append(entry.getFirst());
            ddl.append(" ");
            ddl.append(relDataType);
        }

        ddl.append(")");

        log.info("CREATE TABLE from column names: {}", ddl);

        return ddl.toString();
    }

    @Test
    public void testPrepareTimeout() {
        StringBuilder stmt = new StringBuilder();

        // Generate a complex query so it takes some to parse and validate.
        // SELECT random-numbers
        // UNION
        // SELECT random-numbers
        // ..
        for (int i = 0; i < 5; i++) {
            if (i > 0) {
                stmt.append(System.lineSeparator());
                stmt.append("UNION");
                stmt.append(System.lineSeparator());
            }

            stmt.append("SELECT ");
            for (int j = 0; j < 100; j++) {
                stmt.append(ThreadLocalRandom.current().nextInt(0, 100));
                stmt.append(", ");
            }
            stmt.setLength(stmt.length() - 2);
        }

        assertThrowsSqlException(Sql.EXECUTION_CANCELLED_ERR, "Query timeout", () -> {
            SqlProperties properties = SqlPropertiesHelper.newBuilder()
                    .set(QueryProperty.QUERY_TIMEOUT, 1L)
                    .build();

            QueryProcessor qryProc = queryProcessor();
            await(qryProc.prepareSingleAsync(properties, null, stmt.toString())).parameterTypes();
        });
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    private static void assertUnexpectedNumberOfParameters(String query, Object... params) {
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Unexpected number of query parameters",
                () -> assertQuery(query).withParams(params).check());
    }

    private List<ParameterType> getParameterTypes(String query, Object... params) {
        QueryProcessor qryProc = queryProcessor();
        return await(qryProc.prepareSingleAsync(SqlQueryProcessor.DEFAULT_PROPERTIES, null, query, params)).parameterTypes();
    }
}
