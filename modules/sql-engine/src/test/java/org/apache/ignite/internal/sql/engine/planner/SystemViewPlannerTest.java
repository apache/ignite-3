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

package org.apache.ignite.internal.sql.engine.planner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.rel.IgniteSystemViewScan;
import org.apache.ignite.internal.sql.engine.schema.CatalogColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.DefaultValueStrategy;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemViewImpl;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptorImpl;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Planner test for system views.
 */
public class SystemViewPlannerTest extends AbstractPlannerTest {

    private static final AtomicInteger SYSTEM_VIEW_ID = new AtomicInteger();

    @Test
    public void testReadAllColumns() throws Exception {
        IgniteSystemView propsView = systemPropsView("SYS_PROPS", "KEY", "VAL");

        assertPlan("SELECT * FROM SYS_PROPS", createSystemSchema(propsView),
                isSystemViewScan("SYS_PROPS").and(v -> ImmutableBitSet.of(0, 1).equals(v.requiredColumns())));
    }

    @Test
    public void testPruneColumns() throws Exception {
        IgniteSystemView propsView = systemPropsView("SYS_PROPS", "KEY", "VAL");

        IgniteSchema schema = createSystemSchema(propsView);

        assertPlan("SELECT key FROM SYS_PROPS", schema,
                isSystemViewScan("SYS_PROPS").and(v -> ImmutableBitSet.of(0).equals(v.requiredColumns())));

        assertPlan("SELECT val FROM SYS_PROPS", schema,
                isSystemViewScan("SYS_PROPS").and(v -> ImmutableBitSet.of(1).equals(v.requiredColumns())));
    }

    @Test
    public void testFilterPushDown() throws Exception {
        IgniteSystemView propsView = systemPropsView("SYS_PROPS", "KEY", "VAL");
        IgniteSchema schema = createSystemSchema(propsView);

        assertPlan("SELECT * FROM SYS_PROPS WHERE key like 'os%'", schema,
                isSystemViewScan("SYS_PROPS")
                        .and(hasExpr(IgniteSystemViewScan::condition, "LIKE($t0, _UTF-8'os%')")));
    }

    @Test
    public void testProjectionPushDown() throws Exception {
        IgniteSystemView propsView = systemPropsView("SYS_PROPS", "KEY", "VAL");

        IgniteSchema schema = createSystemSchema(propsView);

        assertPlan("SELECT key, SUBSTRING(val, 1) FROM SYS_PROPS", schema,
                isSystemViewScan("SYS_PROPS")
                        .and(hasExprs(IgniteSystemViewScan::projects, "$t0", "SUBSTRING($t1, 1)")));
    }

    @Test
    public void testPruneColumnsPushProjectionsAndFilters() throws Exception {
        IgniteSystemView propsView = systemPropsView("USERS", "ID", "HOME", "NAME", "TTY");

        IgniteSchema schema = createSystemSchema(propsView);

        assertPlan("SELECT id, SUBSTRING(home, 4) FROM users WHERE name LIKE 'test%'", schema,
                isSystemViewScan("USERS")
                        .and(v -> ImmutableBitSet.of(0, 1, 2).equals(v.requiredColumns()))
                        .and(hasExprs(IgniteSystemViewScan::projects, "$t0", "SUBSTRING($t1, 4)"))
                        .and(hasExpr(IgniteSystemViewScan::condition, "LIKE($t2, _UTF-8'test%')")
        ));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "INSERT INTO SYSTEM.SYS_PROPS VALUES('a', 'b')",
            "INSERT into SYS_PROPS(key, VAL) VALUES('a', 'b')",
            "UPDATE SYSTEM.SYS_PROPS SET val = '0000'",
            "DELETE FROM SYSTEM.SYS_PROPS",
            "MERGE INTO SYSTEM.SYS_PROPS dst USING SYSTEM.SYS_PROPS src ON dst.key = src.key "
                    + "WHEN MATCHED THEN UPDATE SET val = 'x'"
                    + "WHEN NOT MATCHED THEN INSERT (key, val) VALUES (src.key, src.val)"
    })
    public void testDmlOperationsAreNotAllowed(String sql) {
        IgniteSystemView propsView = systemPropsView("SYS_PROPS", "KEY", "VAL");

        CalciteContextException err = assertThrows(CalciteContextException.class, () -> {
            physicalPlan(sql, createSystemSchema(propsView));
        });
        assertThat(err.getMessage(), containsString("System view SYSTEM.SYS_PROPS is not modifiable"));
    }

    private static IgniteSchema createSystemSchema(IgniteSystemView view) {
        return new IgniteSchema("SYSTEM", 1, List.of(view));
    }

    private static IgniteSystemView systemPropsView(String name, String... columnNames) {
        List<ColumnDescriptor> columns = new ArrayList<>(columnNames.length);

        for (String columnName : columnNames) {
            DefaultValueStrategy defaultNull = DefaultValueStrategy.DEFAULT_NULL;
            columns.add(new CatalogColumnDescriptor(columnName, false, false, columns.size(),
                    ColumnType.STRING, 0, 0, 100, defaultNull, null));
        }

        int id = SYSTEM_VIEW_ID.incrementAndGet();
        TableDescriptorImpl tableDescriptor = new TableDescriptorImpl(columns, IgniteDistributions.single());

        return new IgniteSystemViewImpl(name, id, tableDescriptor);
    }

    private static <T extends RelNode> Predicate<T> hasExpr(Function<T, RexNode> expr, String... expectedExprs) {
        return hasExprs((node) -> {
            RexNode oneExpr = expr.apply(node);
            if (oneExpr == null) {
                return List.of();
            } else {
                return List.of(oneExpr);
            }
        }, expectedExprs);
    }

    private static <T extends RelNode> Predicate<T> hasExprs(Function<T, List<RexNode>> exprs, String... expectedExprs) {
        return node -> {
            List<RexNode> exprList = exprs.apply(node);

            if (exprList == null) {
                return expectedExprs.length == 0;
            } else {
                List<String> exprStrList = exprList.stream().map(RexNode::toString).collect(Collectors.toList());
                return exprStrList.equals(List.of(expectedExprs));
            }
        };
    }
}
