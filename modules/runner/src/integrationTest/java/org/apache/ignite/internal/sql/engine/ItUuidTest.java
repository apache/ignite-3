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

import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link org.apache.ignite.internal.sql.engine.type.UuidType} data type.
 */
public class ItUuidTest extends AbstractBasicIntegrationTest {

    // UUID1 > UUID2
    private static final UUID UUID_1 = UUID.fromString("fd10556e-fc27-4a99-b5e4-89b8344cb3ce");

    private static final UUID UUID_2 = UUID.fromString("c67d4baf-564e-4abe-aad5-fcf078d178bf");

    /**
     * Drops all created tables.
     */
    @AfterAll
    public void dropTables() {
        dropAllTables();
    }

    @BeforeAll
    public void createTables() {
        sql("CREATE TABLE t(id INTEGER PRIMARY KEY, uuid_key UUID)");
    }

    @BeforeEach
    public void cleanTables() {
        sql("DELETE FROM t");
    }

    @Test
    public void testUuidLiterals() {
        UUID uuid1 = UUID.randomUUID();

        assertQuery(format("SELECT CAST('{}' as UUID)", uuid1)).columnTypes(UUID.class).returns(uuid1).check();
        assertQuery(format("SELECT '{}'::UUID", uuid1)).columnTypes(UUID.class).returns(uuid1).check();
        // from a dynamic parameter
        assertQuery("SELECT ?").withParams(uuid1).columnTypes(UUID.class).returns(uuid1).check();
    }

    @Test
    public void testTypeOf() {
        UUID uuid1 = UUID.randomUUID();

        assertQuery("SELECT typeof(CAST(? as UUID))").withParams(uuid1.toString()).returns("UUID").check();
        assertQuery("SELECT typeof(?)").withParams(uuid1).returns("UUID").check();

        // https://issues.apache.org/jira/browse/IGNITE-18761
        // TypeOf can short-circuit only when its argument is a constant expression.
        // assertThrows(RuntimeException.class, () -> {
        //   assertQuery("SELECT typeof(CAST('%s' as UUID))").returns("UUID1").check();
        // });
    }

    @Test
    public void testUuidTypeComparison() {
        checkUuidComparison(UUID_1, UUID_2);
    }

    @Test
    public void testUuidInUpdates() {
        sql("INSERT INTO t VALUES (1, ?)", UUID_1);

        // Update column UUID with an UUID value
        assertQuery("UPDATE t SET uuid_key = ? WHERE id=1").withParams(UUID_2).returns(1L).check();
        assertQuery("SELECT uuid_key FROM t WHERE id=1").columnTypes(UUID.class).returns(UUID_2).check();

        // Insert column UUID with a string value are possible to allow CASTs from STRING to UUID.
        sql("INSERT INTO t VALUES (3, ?)", UUID_1.toString());

        // Update column UUID with a string value are possible to allow CASTs from STRING to UUID.
        assertQuery("UPDATE t SET uuid_key = ? WHERE id=1").withParams(UUID_1.toString()).returns(1L).check();
        assertQuery("SELECT uuid_key FROM t WHERE id=1").columnTypes(UUID.class).returns(UUID_1).check();
    }

    @Test
    public void testUuidInQueries() {
        // cast from string
        sql(format("INSERT INTO t VALUES (1, CAST('{}' as UUID))", UUID_1));

        assertQuery("SELECT uuid_key FROM t WHERE id=1").columnTypes(UUID.class).returns(UUID_1).check();

        // parameter in a query
        sql("INSERT INTO t VALUES (2, ?)", UUID_2);
        assertQuery("SELECT uuid_key FROM t WHERE id=2").columnTypes(UUID.class).returns(UUID_2).check();

        // null value
        sql("INSERT INTO t VALUES (3, NULL)");
        assertQuery("SELECT uuid_key FROM t WHERE id=3").columnTypes(UUID.class).returns(null).check();

        // uuid in predicate
        assertQuery("SELECT id FROM t WHERE uuid_key=? ORDER BY id").withParams(UUID_1).returns(1).check();

        // UUID works in comparison operators
        assertQuery("SELECT id, uuid_key FROM t WHERE uuid_key < ? ORDER BY id").withParams(UUID_1)
                .returns(2, UUID_2)
                .check();

        assertQuery("SELECT id, uuid_key FROM t WHERE uuid_key > ? ORDER BY id").withParams(UUID_2)
                .returns(1, UUID_1)
                .check();

        // Works in IN operator
        assertQuery("SELECT id, uuid_key FROM t WHERE uuid_key IN (?, ?) ORDER BY id")
                .withParams(UUID_1, UUID_2)
                .returns(1, UUID_1)
                .returns(2, UUID_2)
                .check();

        // UUID can be used by several aggregate functions
        for (var func : Arrays.asList("COUNT", "ANY_VALUE")) {
            sql(format("SELECT {}(uuid_key) FROM t", func));
        }
    }

    @Test
    public void testUuidCaseExpression() {
        UUID other = UUID.randomUUID();

        sql("INSERT INTO t VALUES (1, ?)", UUID_1);
        sql("INSERT INTO t VALUES (2, ?)", UUID_2);
        sql("INSERT INTO t VALUES (3, NULL)");

        // CASE WHEN <condition> THEN .. WHEN <condition2> THEN ... END
        assertQuery("SELECT id, CASE WHEN uuid_key = ? THEN uuid_key ELSE ? END FROM t ORDER BY id ASC ")
                .withParams(UUID_1, other)
                .returns(1, UUID_1)
                .returns(2, other)
                .returns(3, other)
                .check();

        // CASE <boolean> WHEN ... END

        var query = "SELECT id, CASE uuid_key WHEN uuid_key = ? THEN uuid_key ELSE ? END FROM t;";
        var t = assertThrows(CalciteContextException.class, () -> sql(query, UUID_1));
        assertThat(t.getMessage(), containsString("Invalid types for comparison: UUID = BOOLEAN"));
    }

    @ParameterizedTest
    @MethodSource("binaryComparisonOperators")
    public void testUuidInvalidOperationsAreRejected(String op) {
        var query = format("SELECT ? {} 1", op);
        var t = assertThrows(IgniteException.class, () -> sql(query, UUID_1));
        assertThat(t.getMessage(), containsString("class java.util.UUID cannot be cast to class java.lang.Integer"));
    }

    private static Stream<String> binaryComparisonOperators() {
        return SqlKind.BINARY_COMPARISON.stream()
                // to support IS DISTINCT FROM/IS NOT DISTINCT FROM
                .map(o -> o.sql.replace("_", " "));
    }

    @ParameterizedTest
    @MethodSource("binaryArithmeticOperators")
    public void testUuidUnsupportedOperators(String op) {
        var query = format("SELECT CAST('{}' as UUID) {} CAST('{}' as UUID)", UUID_1, op, UUID_1);
        var t = assertThrows(IgniteException.class, () -> sql(query));
        var errorMessage = format("Invalid types for arithmetic: class java.util.UUID {} class java.util.UUID", op);
        assertThat(t.getMessage(), containsString(errorMessage));
    }

    private static Stream<String> binaryArithmeticOperators() {
        return Stream.of("+", "-", "/", "*");
    }

    @Test
    public void testUuidInvalidCasts() {
        // invalid cast from integer to UUID an error from expression runtime.
        // A call to UuidFunctions::cast is generates by RexToLitTranslator::translateCast
        // org.apache.ignite.internal.sql.engine.type.UuidFunctions.cast(int)
        {
            var t = assertThrows(IgniteException.class, () -> sql("SELECT CAST(1 as UUID)"));
            assertThat(t.getMessage(), containsString("class java.lang.Integer cannot be cast to class java.util.UUID"));
        }

        // A call to UuidFunctions::cast is generated by ConverterUtils::convert
        // invalid cast from integer to UUID
        {
            var t = assertThrows(RuntimeException.class, () -> sql("SELECT CAST(? as UUID)", 1));
            assertThat(t.getMessage(), containsString("class java.lang.Integer cannot be cast to class java.util.UUID"));
        }
    }

    @Test
    public void testUuidTypeCoercion() {
        assertQuery(format("SELECT CAST('{}' as UUID) = '{}'", UUID_1, UUID_1)).returns(true).check();
        assertQuery(format("SELECT '{}' = CAST('{}' as UUID)", UUID_1, UUID_1)).returns(true).check();

        assertQuery(format("SELECT '{}'::UUID = '{}'", UUID_1, UUID_1)).returns(true).check();
        assertQuery(format("SELECT '{}'= '{}'::UUID", UUID_1, UUID_1)).returns(true).check();

        // UUID / String
        checkUuidComparison(UUID_1.toString(), UUID_2);

        // String / UUID
        checkUuidComparison(UUID_1, UUID_2.toString());
    }

    @Test
    public void testTypeCoercionInDml() {
        sql("INSERT INTO t VALUES (1, ?)",  UUID_1.toString());

        sql("UPDATE t SET uuid_key=? WHERE id=1", UUID_2.toString());

        assertQuery("SELECT id, uuid_key FROM t WHERE id = 1")
                .returns(1,  UUID_2)
                .check();
    }

    private void checkUuidComparison(Object uuid1, Object uuid2) {
        assertQuery("SELECT ? = ?").withParams(uuid1, uuid1).returns(true).check();
        assertQuery("SELECT ? != ?").withParams(uuid1, uuid1).returns(false).check();

        assertQuery("SELECT ? = ?").withParams(uuid1, uuid2).returns(false).check();
        assertQuery("SELECT ? != ?").withParams(uuid1, uuid2).returns(true).check();

        assertQuery("SELECT ? > ?").withParams(uuid2, uuid1).returns(false).check();
        assertQuery("SELECT ? >= ?").withParams(uuid2, uuid1).returns(false).check();
        assertQuery("SELECT ? < ?").withParams(uuid2, uuid1).returns(true).check();
        assertQuery("SELECT ? <= ?").withParams(uuid2, uuid1).returns(true).check();

        assertQuery("SELECT ? > ?").withParams(uuid1, uuid2).returns(true).check();
        assertQuery("SELECT ? >= ?").withParams(uuid1, uuid2).returns(true).check();
        assertQuery("SELECT ? < ?").withParams(uuid1, uuid2).returns(false).check();
        assertQuery("SELECT ? <= ?").withParams(uuid1, uuid2).returns(false).check();
    }
}
