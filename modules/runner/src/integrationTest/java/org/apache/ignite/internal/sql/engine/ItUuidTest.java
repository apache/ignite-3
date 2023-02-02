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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.base.CharMatcher;
import java.util.Arrays;
import java.util.UUID;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

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
        var igniteTables = CLUSTER_NODES.get(0).tables();

        for (var table : igniteTables.tables()) {
            sql("DROP TABLE " + table.name());
        }
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

        assertQuery(String.format("SELECT CAST('%s' as UUID)", uuid1)).columnTypes(UUID.class).returns(uuid1).check();
        assertQuery(String.format("SELECT '%s'::UUID", uuid1)).columnTypes(UUID.class).returns(uuid1).check();
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

        // Update column UUID with a string value it should be possible
        // since we are adding implicit casts.
        assertQuery("UPDATE t SET uuid_key = ? WHERE id=1").withParams(UUID_1.toString()).returns(1L).check();
        assertQuery("SELECT uuid_key FROM t WHERE id=1").columnTypes(UUID.class).returns(UUID_1).check();
    }

    @Test
    public void testUuidInQueries() {
        // cast from string
        sql(String.format("INSERT INTO t VALUES (1, CAST('%s' as UUID))", UUID_1));

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
            sql(String.format("SELECT %s(uuid_key) FROM t", func));
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

        checkOperatorIsNotSupported(assertQuery(
                "SELECT id, CASE uuid_key WHEN uuid_key = ? THEN uuid_key ELSE ? END FROM t ORDER BY id ASC")
                .withParams(UUID_1, other), SqlTypeName.BOOLEAN);

        // CASE WHEN <condition> THEN .. WHEN <condition2> THEN ... END

        checkOperatorIsNotSupported(assertQuery("SELECT id, CASE ? WHEN uuid_key = ? THEN uuid_key ELSE ? END FROM t ORDER BY id ASC ")
                .withParams(UUID_1, UUID_2, other), SqlTypeName.BOOLEAN);

        // triggers
        // class org.apache.calcite.sql.SqlDynamicParam: ?
        // java.lang.UnsupportedOperationException: class org.apache.calcite.sql.SqlDynamicParam: ?
        //   at org.apache.calcite.util.Util.needToImplement(Util.java:1101)

        checkOperatorIsNotSupported(assertQuery("SELECT CASE ? "
                + "WHEN CAST('c67d4baf-564e-4abe-aad5-fcf078d178bf' as UUID) = ? "
                + "THEN CAST('c67d4baf-564e-4abe-aad5-fcf078d178bf' as UUID) "
                + "ELSE ? END")
                .withParams(UUID_1, UUID_2, other), SqlTypeName.BOOLEAN);
    }

    @Test
    public void testUuidSupportedOperations() {
        for (var op : IgniteSqlOperatorTable.INSTANCE.getOperatorList()) {
            if (CharMatcher.inRange('A', 'Z').or(CharMatcher.inRange('a', 'z')).matchesAnyOf(op.getName())) {
                continue;
            }

            SqlKind kind = op.getKind();
            if (!SqlKind.BINARY_COMPARISON.contains(kind) || !SqlKind.BINARY_ARITHMETIC.contains(kind)) {
                continue;
            }

            var query = String.format("SELECT ? %s 1", op.getName());
            checkOperatorIsNotSupported(assertQuery(query).withParams(UUID_1), SqlTypeName.BOOLEAN);
        }
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
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18762")
    public void testUuidTypeCoercion() {
        assertQuery(String.format("SELECT CAST('%s' as UUID) = '%s'", UUID_1, UUID_2)).returns(true).check();
        assertQuery(String.format("SELECT '%s' = CAST('%s' as UUID)", UUID_1, UUID_2)).returns(true).check();

        assertQuery(String.format("SELECT '%s'::UUID = '%s'", UUID_1, UUID_1)).returns(true).check();
        assertQuery(String.format("SELECT '%s'= '%s'::UUID", UUID_1, UUID_1)).returns(true).check();

        // UUID / String
        checkUuidComparison(UUID_1.toString(), UUID_2);

        // String / UUID
        checkUuidComparison(UUID_1, UUID_2.toString());
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18762")
    public void testTypeCoercionInDml() {
        sql("CREATE TABLE t(id INTEGER PRIMARY KEY, uuid_key UUID)");
        sql("INSERT INTO t VALUES (1, ?)",  UUID_1.toString());

        sql("CREATE TABLE t(id INTEGER PRIMARY KEY, uuid_key UUID)");
        sql("UPDATE t SET uuid_key=? WHERE id=1",  UUID_2.toString(), 1);

        assertQuery("SELECT id, uuid_key FROM t WHERE id = 1").withParams(UUID_2.toString())
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

        assertQuery("SELECT ? > ?").withParams(uuid1, uuid2).returns(true).check();
        assertQuery("SELECT ? >= ?").withParams(uuid1, uuid2).returns(true).check();
    }

    private void checkOperatorIsNotSupported(QueryChecker checker, SqlTypeName other) {
        var t = assertThrows(CalciteContextException.class, checker::check);
        assertThat(t.getMessage(), containsString("There is no operator UUID = " + other));
    }
}
