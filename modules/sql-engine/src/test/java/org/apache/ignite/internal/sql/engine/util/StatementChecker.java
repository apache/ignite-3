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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteValues;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.util.Pair;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeMatcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AssertionFailureBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.opentest4j.AssertionFailedError;

/**
 * Constructs SQL statements checks as junit {@link DynamicTest}.
 *
 * <pre>
 *     // Checks that plan validation throws no errors.
 *     new StatementChecker().sql("SELECT 1 + 1").ok();
 *
 *     // Checks that plan validation throws no errors.
 *     new StatementChecker().sql("SELECT 1 + 1").ok(node -> {
 *         // Validation function
 *         Assertion.assertTrue(true);
 *     });
 *
 *     // Checks that plan validation fails.
 *     new StatementChecker().sql("SELECT").fails();
 *
 *     // Checks that plan validation fails and a error message contains the given string.
 *     new StatementChecker().sql("SELECT").fails("Parse error);
 *
 * </pre>
 *
 * <p><b>Test display names</b>
 *
 * <p>Default test name consists of expected result {@code OK} if test should pass and {@code ERR} if it should fail,
 * an SQL statement string, and dynamic parameters.
 * <pre>
 *     new StatementChecker().sql("SELECT 1").ok(); // OK SELECT 1
 *     new StatementChecker().sql("SELECT t").fails(); // ERR SELECT t
 *     new StatementChecker().sql("SELECT ?", 1).ok(); // OK SELECT 1, params=1
 * </pre>
 *
 * <p><b>Schema initialization</b>
 *
 * <pre>
 *     new StatementChecker()
 *         .table("t1", "int_col", NativeTypes.INT32)
 *         .sql("SELECT int_col FROM t1")
 *         .ok();
 * </pre>
 *
 * <p><b>Additional checks</b>
 * <pre>
 *     // Checks that plan validation throws no errors and plan's topmost projection contains the given expressions.
 *     new StatementChecker().sql("SELECT 1, 2").project("1", "2");
 * </pre>
 */
public class StatementChecker {

    private String sqlStatement;

    private List<Object> dynamicParams;

    private final SqlPrepare sqlPrepare;

    private final Map<String, Function<TestBuilders.TableBuilder, IgniteTable>> testTables = new HashMap<>();

    private boolean dumpPlan;

    private String[] rulesToDisable = new String[0];

    private Consumer<StatementChecker> setup = (checker) -> {};

    private List<RelDataType> expectedParameterTypes;

    private BiFunction<String, RelNode, String> planToString = (header, plan) -> {
        return RelOptUtil.dumpPlan(
                header, plan,
                SqlExplainFormat.TEXT, SqlExplainLevel.NON_COST_ATTRIBUTES);
    };

    public StatementChecker(SqlPrepare sqlPrepare) {
        this.sqlPrepare = sqlPrepare;
    }

    /** Prepares the given SQL statement string. */
    @FunctionalInterface
    public interface SqlPrepare {

        /**
         * Validates and converts the given SQL statement into a physical plan.
         *
         * @param schema A schema.
         * @param sql An SQL statement.
         * @param params A list of dynamic parameters.
         * @param rulesToDisable A list of rules to exclude from optimisation.
         */
        Pair<IgniteRel, IgnitePlanner> prepare(
                IgniteSchema schema, String sql, List<Object> params, String... rulesToDisable
        ) throws Exception;
    }

    /** Sets a function that is going to be called prior to test run. */
    public StatementChecker setup(Consumer<StatementChecker> setup) {
        this.setup = setup;
        return this;
    }

    /** Sets rules to exclude from optimisation. */
    public StatementChecker disableRules(String... rulesToDisable) {
        this.rulesToDisable = rulesToDisable;

        return this;
    }

    /**
     * Updates schema to include a table with 1 column.
     */
    public StatementChecker table(String tableName, String colName, NativeType colType) {
        testTables.put(tableName, (table) -> {
            return table.name(tableName.toUpperCase(Locale.US))
                    .addColumn(colName.toUpperCase(Locale.US), colType)
                    .distribution(IgniteDistributions.single())
                    .build();
        });

        return this;
    }

    /**
     * Updates schema to include a table with 1 column.
     */
    public StatementChecker table(String tableName, Function<TestBuilders.TableBuilder, IgniteTable> table) {
        testTables.put(tableName, table);

        return this;
    }

    /**
     * Updates schema to include a table with 2 columns.
     */
    public StatementChecker table(String tableName,
            String col1, NativeType type1,
            String col2, NativeType type2) {

        testTables.put(tableName, table -> {
            return table.name(tableName.toUpperCase(Locale.US))
                    .addColumn(col1.toUpperCase(Locale.US), type1)
                    .addColumn(col2.toUpperCase(Locale.US), type2)
                    .distribution(IgniteDistributions.single())
                    .build();
        });

        return this;
    }

    /**
     * Updates schema to include a table with 3 columns.
     */
    public StatementChecker table(String tableName,
            String col1, NativeType type1,
            String col2, NativeType type2,
            String col3, NativeType type3) {

        testTables.put(tableName, table -> {
            return table.name(tableName.toUpperCase(Locale.US))
                    .addColumn(col1.toUpperCase(Locale.US), type1)
                    .addColumn(col2.toUpperCase(Locale.US), type2)
                    .addColumn(col3.toUpperCase(Locale.US), type3)
                    .distribution(IgniteDistributions.single())
                    .build();
        });

        return this;
    }

    /** Sets an SQL statement string and dynamic parameters. */
    public StatementChecker sql(String template, Object... params) {
        return sql(() -> template, params);
    }

    /**
     * Sets an SQL statement string and dynamic parameters.
     * Existing to make writing multiline SQL statements easier.
     */
    public StatementChecker sql(Supplier<String> template, Object... params) {
        this.sqlStatement = template.get();
        this.dynamicParams = Arrays.asList(params);
        return this;
    }

    /** Specifies expected types of dynamic parameters. Use {@code null} to represent {@link SqlTypeName#NULL}. */
    public StatementChecker parameterTypes(NativeType... types) {
        this.expectedParameterTypes = Arrays.stream(types)
                .map(t -> {
                    if (t == null) {
                        return Commons.typeFactory().createSqlType(SqlTypeName.NULL);
                    } else {
                        return TypeUtils.native2relationalType(Commons.typeFactory(), t);
                    }
                }).collect(Collectors.toList());
        return this;
    }

    public StatementChecker parameterTypes(RelDataType... types) {
        this.expectedParameterTypes = Arrays.asList(types);
        return this;
    }

    /** Expect that validation succeeds. */
    public DynamicTest ok() {
        return ok((node) -> {}, true);
    }

    /** Expect that validation succeeds. */
    // TODO: remote relCheck param after https://issues.apache.org/jira/browse/IGNITE-20170
    public DynamicTest ok(boolean relCheck) {
        return ok((node) -> {}, relCheck);
    }

    /**
     * Expects that the provided validation function won't throw an exception.
     */
    public DynamicTest ok(Consumer<IgniteRel> check) {
        String name = testName(true);
        // Capture current stacktrace to show error location.
        AssertionError exception = new AssertionError("Statement check failed");

        return shouldPass(name, exception, check, true);
    }

    /**
     * Expects that the provided validation function won't throw an exception.
     */
    public DynamicTest ok(Consumer<IgniteRel> check, boolean relCheck) {
        String name = testName(true);
        // Capture current stacktrace to show error location.
        AssertionError exception = new AssertionError("Statement check failed");

        return shouldPass(name, exception, check, relCheck);
    }

    /** Validation is expected to fail with an error that contains a the given message. */
    public DynamicTest fails() {
        String name = testName(false);
        // Capture current stacktrace to show error location.
        AssertionError exception = new AssertionError("Statement check failed");

        return shouldFail(name, exception, CoreMatchers.anything("Any exception"));
    }

    /** Validation is expected to fail and error should match. */
    public DynamicTest fails(Matcher<? super Throwable> matcher) {
        String name = testName(false);
        // Capture current stacktrace to show error location.
        AssertionError exception = new AssertionError("Statement check failed");

        return shouldFail(name, exception, matcher);
    }

    /** Validation is expected to fail with an error that contains a the given message. */
    public DynamicTest fails(String errorMessage) {
        String name = testName(false);
        // Capture current stacktrace to show error location.
        AssertionError exception = new AssertionError("Statement check failed");

        // please do not replace generic in TypeSafeMatcher with diamond.
        // Sometimes IDEA goes crazy and start throwing error on compilation                                  V
        return shouldFail(name, exception, new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(Throwable t) {
                if (t.getMessage() == null) {
                    return false;
                }
                return t.getMessage().contains(errorMessage);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Error message should include ").appendValue(errorMessage);
            }

            @Override
            protected void describeMismatchSafely(Throwable item, Description mismatchDescription) {
                describeTo(mismatchDescription);
                mismatchDescription.appendText(" was ").appendValue(item.getMessage());
            }
        });
    }

    /**
     * Checks that a topmost operator with a projection list has projections that match the provided projection list.
     * <ul>
     *     <li>{@link IgniteProject}</li>
     *     <li>{@link IgniteTableScan}</li>
     *     <li>{@link IgniteIndexScan}</li>
     *     <li>{@link IgniteValues} with exactly one row.</li>
     * </ul>
     */
    public DynamicTest project(String... exprs) {
        String expected;
        if (exprs.length != 0) {
            expected = Arrays.asList(exprs).toString();
        } else {
            expected = null;
        }

        return ok(node -> {
            do {
                if (node instanceof IgniteTableScan) {
                    expectProjection((IgniteTableScan) node, expected, IgniteTableScan::projects);
                    return;
                } else if (node instanceof IgniteProject) {
                    expectProjection((IgniteProject) node, expected, IgniteProject::getProjects);
                    return;
                } else if (node instanceof IgniteValues) {
                    IgniteValues values = ((IgniteValues) node);
                    assertEquals(1, values.tuples.size(), "Unable check projection list. Number of rows does not match:\n" + values);
                    expectProjection((IgniteValues) node, expected, (in) -> values.tuples.get(0));
                    return;
                } else if (node instanceof IgniteIndexScan) {
                    expectProjection((IgniteIndexScan) node, expected, ProjectableFilterableTableScan::projects);
                    return;
                }

                if (node.getInputs().isEmpty()) {
                    break;
                }

                node = (IgniteRel) node.getInput(0);
            } while (true);

            Assertions.fail("No nodes that contain projection. Expected projection: " + expected);
        });
    }

    /**
     * Prints plan, parameters, SQL to stdout after preparing a plan.
     */
    public StatementChecker dumpPlan() {
        this.dumpPlan = true;
        return this;
    }

    /** Sets a function that convert plan into a string. A function accepts a header and a plan. */
    public StatementChecker planToString(BiFunction<String, RelNode, String> planToString) {
        this.planToString = planToString;
        return this;
    }

    /** Executes default checks. Runs before a check provide by {@link StatementChecker#ok(Consumer)}. */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected void checkRel(IgniteRel igniteRel, IgnitePlanner planner, IgniteSchema schema) {

    }

    private IgniteSchema createSchema() {
        List<IgniteTable> tables = new ArrayList<>();
        for (Map.Entry<String, Function<TestBuilders.TableBuilder, IgniteTable>> entry : testTables.entrySet()) {
            String tableName = entry.getKey();
            Function<TableBuilder, IgniteTable> addTable = entry.getValue();

            IgniteTable table = addTable.apply(TestBuilders.table().name(tableName));

            tables.add(table);
        }

        return new IgniteSchema(SqlCommon.DEFAULT_SCHEMA_NAME, 0, tables);
    }

    private DynamicTest shouldPass(String name, Throwable exception, Consumer<IgniteRel> check, boolean relCheck) {
        return DynamicTest.dynamicTest(name, () -> {
            IgniteSchema schema = initSchema(exception);
            IgniteRel root;
            IgnitePlanner planner;

            try {
                Pair<IgniteRel, IgnitePlanner> result = sqlPrepare.prepare(schema, sqlStatement, dynamicParams, rulesToDisable);
                root = result.getFirst();
                planner = result.getSecond();

                if (relCheck) {
                    checkRel(root, planner, schema);
                }
            } catch (Throwable e) {
                String message = format("Failed to validate:\n{}\n", formatSqlStatementForErrorMessage());
                RuntimeException error = new RuntimeException(message, e);
                error.addSuppressed(exception);

                throw error;
            }

            reportPlan(root);

            try {
                check.accept(root);

                checkParameterTypes(planner);
            } catch (Throwable e) {
                String planDump = buildPlanInfo(root);
                String mismatchDescription = format("Plan does not match:\n\n{}", planDump);

                // dump SQL, dynamic params to std err
                System.err.println(mismatchDescription);

                // include test location
                e.addSuppressed(exception);
                throw e;
            }
        });
    }

    private IgniteSchema initSchema(Throwable exception) {
        IgniteSchema schema;

        try {
            this.setup.accept(this);

            schema = createSchema();
        } catch (Exception e) {
            IllegalStateException error = new IllegalStateException("Failed to initialise", e);
            // include test location
            error.addSuppressed(exception);
            throw error;
        }

        return schema;
    }

    private DynamicTest shouldFail(String name, Throwable exception, Matcher<? super Throwable> matcher) {
        return DynamicTest.dynamicTest(name, () -> {
            IgniteSchema schema = initSchema(exception);

            Throwable err = null;
            IgniteRel unexpectedPlan = null;
            try {
                Pair<IgniteRel, ?> unexpected = sqlPrepare.prepare(schema, sqlStatement, dynamicParams, rulesToDisable);
                unexpectedPlan = unexpected.getFirst();
            } catch (Throwable t) {
                err = t;
            }

            if (err != null) {
                if (!matcher.matches(err)) {
                    StringDescription desc = new StringDescription();
                    matcher.describeMismatch(err, desc);

                    AssertionFailedError error = AssertionFailureBuilder.assertionFailure()
                            .reason("Error does not match")
                            .actual(err)
                            .expected(desc)
                            .cause(err)
                            .build();

                    // Include test location
                    error.addSuppressed(exception);

                    throw error;
                }
            } else {
                String planDump = buildPlanInfo(unexpectedPlan);
                String mismatchDescription = format("Plan should not be possible:\n\n{}", planDump);

                AssertionFailureBuilder.assertionFailure()
                        .reason(mismatchDescription)
                        // Include test location
                        .cause(exception)
                        .buildAndThrow();
            }
        });
    }

    private String testName(boolean ok) {
        String name = dynamicParams.isEmpty() ? sqlStatement : sqlStatement + ", params=" + dynamicParams;
        if (ok) {
            return "OK " + name;
        } else {
            return "ERR " + name;
        }
    }

    private void reportPlan(IgniteRel root) {
        if (!dumpPlan) {
            return;
        }
        String s = buildPlanInfo(root);
        System.err.println(s);
    }

    private String buildPlanInfo(RelNode plan) {
        String header = formatSqlStatementForErrorMessage();

        return planToString.apply(header, plan);
    }

    private String formatSqlStatementForErrorMessage() {
        String header;

        if (!dynamicParams.isEmpty()) {
            header = format("{}\n\nDynamic parameters: [{}]\n", sqlStatement, dynamicParams.toString());
        } else {
            header = format("{}\n", sqlStatement);
        }
        return header;
    }

    private static <T extends IgniteRel> void expectProjection(T igniteRel, @Nullable String expected,
            Function<T, List<? extends RexNode>> projectionFunc) {

        List<? extends RexNode> projection = projectionFunc.apply(igniteRel);
        String projectionString = projection != null ? projection.toString() : null;

        assertEquals(expected, projectionString, "Projection list does not match");
    }

    private void checkParameterTypes(IgnitePlanner planner) {
        if (expectedParameterTypes != null) {
            RelDataType parameterRowType = planner.getParameterRowType();
            // Compare string representation because that representation includes nullability attribute.

            List<String> actualTypes = parameterRowType.getFieldList().stream()
                    .map(RelDataTypeField::getType)
                    .map(RelDataType::getFullTypeString)
                    .collect(Collectors.toList());

            List<String> expectedTypes = expectedParameterTypes.stream()
                    .map(RelDataType::getFullTypeString)
                    .collect(Collectors.toList());

            assertEquals(expectedTypes, actualTypes, "Parameters do not match");
        }
    }
}

