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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.ParameterMetadata;
import org.apache.ignite.internal.sql.engine.prepare.PlanId;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruner;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPrunerImpl;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.util.EmptyCacheFactory;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A runner for mapping test cases.
 *
 * <p>Test case format:
 * <pre>
 *     #&lt;optional multiline description&gt;
 *     &lt;node_name&gt;
 *     ---
 *     &lt;SQL statement (single line or multiline)&gt;
 *     ---
 *     &lt;expected fragments&gt;
 *     ---
 * </pre>
 *
 * <p>To bulk update test case results set system property {@code MAPPING_TESTS_OVERWRITE_RESULTS} to {@code true}:
 *
 * <p>As a result all test cases will have results that match actual ones.
 * If there are mismatches tests still fail to report and possibly review them.
 *
 * <p>To remove all results set system property {@code MAPPING_TESTS_STRIP_RESULTS} to {@code true}:
 *
 * <p>As a result all test cases' results will be replaced with a dummy result.
 * If there are mismatches tests still fail to report and possibly review them.
 *
 * @see FragmentPrinter
 */
final class MappingTestRunner {

    private static final String OVERWRITE_RESULTS = "MAPPING_TESTS_OVERWRITE_RESULTS";

    private static final String STRIP_RESULTS = "MAPPING_TESTS_STRIP_RESULTS";

    /** Test setup. */
    static final class TestSetup {

        private final ExecutionTargetProvider executionTargetProvider;

        private final IgniteSchema schema;

        private final LogicalTopologySnapshot topologySnapshot;

        TestSetup(ExecutionTargetProvider executionTargetProvider, IgniteSchema schema, LogicalTopologySnapshot topologySnapshot) {
            this.executionTargetProvider = executionTargetProvider;
            this.schema = schema;
            this.topologySnapshot = topologySnapshot;
        }
    }

    private final Path location;

    private final BiFunction<IgniteSchema, String, IgniteRel> parseValidate;

    private boolean overwriteResults;

    private boolean stripResults;

    private Path outputFile;

    MappingTestRunner(Path location,
            BiFunction<IgniteSchema, String, IgniteRel> parseValidate) {

        this.location = location;
        this.parseValidate = parseValidate;
    }

    /**
     * Specifies a location to write results to. If not specified, all results are written to a test file,
     *
     * @param path Location
     * @return this for chaining.
     */
    MappingTestRunner outputFile(Path path) {
        this.outputFile = path;
        return this;
    }

    void runTest(Supplier<TestSetup> init, String fileName) {
        TestSetup setup = init.get();

        initFlags();

        Path testFile = location.resolve(fileName);
        List<TestCaseDef> testCases = loadTestCases(testFile);

        runTestCases(testFile, setup.schema, setup.executionTargetProvider, setup.topologySnapshot, parseValidate, testCases);
    }

    @TestOnly
    void initFlags() {
        overwriteResults = IgniteSystemProperties.getBoolean(OVERWRITE_RESULTS, false);
        stripResults = IgniteSystemProperties.getBoolean(STRIP_RESULTS, false);

        if (overwriteResults && stripResults) {
            throw new IllegalStateException(format("Both {} and {} have been specified", OVERWRITE_RESULTS, STRIP_RESULTS));
        }
    }

    private void runTestCases(Path testFile,
            IgniteSchema schema,
            ExecutionTargetProvider targetProvider,
            LogicalTopologySnapshot snapshot,
            BiFunction<IgniteSchema, String, IgniteRel> parse,
            List<TestCaseDef> testCases) {

        List<String> actualResults = new ArrayList<>();

        for (TestCaseDef testDef : testCases) {
            IgniteRel rel = parse.apply(schema, testDef.sql);
            SqlQueryType sqlQueryType;
            if (rel instanceof IgniteTableModify) {
                sqlQueryType = SqlQueryType.DML;
            } else {
                sqlQueryType = SqlQueryType.QUERY;
            }
            ResultSetMetadataImpl resultSetMetadata = new ResultSetMetadataImpl(Collections.emptyList());
            ParameterMetadata parameterMetadata = new ParameterMetadata(Collections.emptyList());
            MultiStepPlan multiStepPlan = new MultiStepPlan(new PlanId(UUID.randomUUID(), 1), sqlQueryType, rel,
                    resultSetMetadata, parameterMetadata, schema.catalogVersion());

            String actualText = produceMapping(testDef.nodeName, targetProvider, snapshot, multiStepPlan);

            actualResults.add(actualText);
        }

        reportResults(testCases, actualResults, testFile);
    }

    static List<TestCaseDef> loadTestCases(Path path) {
        try {
            Parser state = new Parser();
            return state.parse(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String produceMapping(String nodeName,
            ExecutionTargetProvider targetProvider,
            LogicalTopologySnapshot snapshot,
            MultiStepPlan plan) {

        PartitionPruner partitionPruner = new PartitionPrunerImpl();
        MappingServiceImpl mappingService = new MappingServiceImpl(nodeName,
                targetProvider,
                EmptyCacheFactory.INSTANCE,
                0,
                partitionPruner,
                Runnable::run
        );
        mappingService.onTopologyLeap(snapshot);

        List<MappedFragment> mappedFragments;

        try {
            mappedFragments = await(mappingService.map(plan, MappingParameters.EMPTY));
        } catch (Exception e) {
            String explanation = System.lineSeparator()
                    + RelOptUtil.toString(plan.root())
                    + System.lineSeparator();

            Throwable cause = e instanceof CompletionException ? e.getCause() : e;

            throw new IllegalStateException("Failed to map a plan: " + explanation, cause);
        }

        if (mappedFragments == null) {
            throw new IllegalStateException("Mapped fragments");
        }

        return FragmentPrinter.fragmentsToString(mappedFragments);
    }

    static class TestCaseDef {

        final int lineNo;

        @Nullable
        final String description;

        final String nodeName;

        final String sql;

        final String result;

        TestCaseDef(int lineNo, @Nullable String description, String nodeName, String sql, String res) {
            this.lineNo = lineNo;
            this.description = description;
            this.nodeName = nodeName;
            this.sql = sql;
            this.result = res;
        }
    }

    void reportResults(List<TestCaseDef> testCases, List<String> actualResults, Path fileName) {
        doReportResults(testCases, actualResults, outputFile != null ? outputFile : fileName);
    }

    private void doReportResults(List<TestCaseDef> testCases, List<String> actualResults, Path outputFile) {
        StringBuilder expected = new StringBuilder();
        StringBuilder actual = new StringBuilder();

        for (int i = 0; i < testCases.size(); i++) {
            TestCaseDef testCaseDef = testCases.get(i);
            String actualResult = actualResults.get(i);

            appendTestCaseStr(i, expected, testCaseDef, testCaseDef.result.stripTrailing());
            appendTestCaseStr(i, actual, testCaseDef, actualResult.stripTrailing());
        }

        if (overwriteResults) {
            try {
                Files.writeString(outputFile, actual.toString(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to update a test file " + outputFile, e);
            }
        } else if (stripResults) {
            StringBuilder stripped = new StringBuilder();

            for (int i = 0; i < testCases.size(); i++) {
                TestCaseDef testCaseDef = testCases.get(i);
                appendTestCaseStr(i, stripped, testCaseDef, "f");
            }

            try {
                Files.writeString(outputFile, stripped.toString(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to update a test file " + outputFile, e);
            }
        }

        if (!Objects.equals(expected.toString(), actual.toString())) {
            String message = format("There are test failures. Location: ({}:1)", outputFile.toAbsolutePath());
            assertEquals(expected.toString(), actual.toString(), message);
        }
    }

    private static void appendTestCaseStr(int idx, StringBuilder testCaseStr, TestCaseDef testCaseDef, String result) {
        // Write description or a blank line (if this is not the first test case).
        if (testCaseDef.description != null) {
            testCaseStr.append(testCaseDef.description);
            testCaseStr.append(System.lineSeparator());
        } else if (idx > 0) {
            testCaseStr.append(System.lineSeparator());
        }

        testCaseStr
                .append(testCaseDef.nodeName)
                .append(System.lineSeparator())
                .append(testCaseDef.sql)
                .append(System.lineSeparator())
                .append("---")
                .append(System.lineSeparator())
                .append(result)
                .append(System.lineSeparator())
                .append("---")
                .append(System.lineSeparator());
    }

    enum ParseState {
        NODE_NAME,
        SQL_STMT,
        FRAGMENTS
    }

    private static class Parser {
        private static final String DELIMITER = "---";
        private static final String COMMENT = "#";
        private ParseState nextState = ParseState.NODE_NAME;
        private int testCaseLineNo;
        private int numFragments;

        private final StringBuilder description = new StringBuilder();
        private String nodeName;
        private final StringBuilder sqlStmt = new StringBuilder();
        private final StringBuilder result = new StringBuilder();

        private void resetState() {
            nextState = ParseState.NODE_NAME;
            testCaseLineNo = -1;
            numFragments = 0;
            sqlStmt.setLength(0);
            result.setLength(0);
            description.setLength(0);
        }

        List<TestCaseDef> parse(Path path) throws IOException {
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            List<TestCaseDef> testCases = new ArrayList<>();

            int lineNo = 1;
            String fileName = path.toAbsolutePath().toString();

            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);

                if (line.isBlank()) {
                    lineNo++;
                    continue;
                }

                switch (nextState) {
                    case NODE_NAME:
                        // Read optional comments into description field
                        if (line.startsWith(COMMENT)) {
                            if (description.length() > 0) {
                                description.append(System.lineSeparator());
                            }

                            description.append(line);
                        } else {
                            StringTokenizer tokenizer = new StringTokenizer(line.stripLeading());

                            this.nodeName = tokenizer.nextToken();
                            this.testCaseLineNo = i;

                            if (tokenizer.hasMoreTokens()) {
                                throw reportError(fileName, lineNo, "Expected mapping <node-name> but got: {}", line);
                            }
                            nextState = ParseState.SQL_STMT;
                        }
                        break;
                    case SQL_STMT:
                        if (line.startsWith(COMMENT)) {
                            throw reportError(fileName, lineNo, "Comments are not allowed in sql statement text", line);
                        }

                        if (!line.stripLeading().startsWith(DELIMITER)) {
                            if (sqlStmt.length() > 0) {
                                sqlStmt.append(System.lineSeparator());
                            }

                            sqlStmt.append(line);
                        } else {
                            nextState = ParseState.FRAGMENTS;
                        }
                        break;
                    case FRAGMENTS:
                        if (line.startsWith(COMMENT)) {
                            throw reportError(fileName, lineNo, "Comments are not allowed in fragment text", line);
                        }

                        if (!line.stripLeading().startsWith(DELIMITER)) {
                            if (result.length() > 0) {
                                result.append(System.lineSeparator());
                            }

                            if (line.startsWith(FragmentPrinter.FRAGMENT_PREFIX)) {
                                if (numFragments > 0) {
                                    result.append(System.lineSeparator());
                                }
                                numFragments++;
                            }

                            result.append(line);
                        } else {
                            TestCaseDef newTestCase = newTestCase();
                            testCases.add(newTestCase);

                            resetState();
                        }
                        break;
                    default:
                        throw new IllegalStateException("Unexpected state id: " + nextState);
                }

                lineNo++;
            }

            if (nextState == ParseState.FRAGMENTS && !result.toString().isBlank()) {
                TestCaseDef newTestCase = newTestCase();
                testCases.add(newTestCase);
            } else if (nextState != ParseState.NODE_NAME) {
                StringBuilder possibleTestCase = new StringBuilder();
                possibleTestCase.append(System.lineSeparator());

                for (int i = this.testCaseLineNo; i < lineNo; i++) {
                    String line = lines.get(i - 1);
                    if (i != this.testCaseLineNo) {
                        possibleTestCase.append(System.lineSeparator());
                    }
                    possibleTestCase.append(line);
                }

                String error = format("Error at ({}:{}): Invalid test case: {}", fileName, this.testCaseLineNo, possibleTestCase);
                throw new IllegalArgumentException(error);
            }

            return testCases;
        }

        private TestCaseDef newTestCase() {
            String sql = sqlStmt.toString();
            String res = result.toString().stripTrailing();
            String desc = description.length() > 0 ? description.toString() : null;

            return new TestCaseDef(testCaseLineNo, desc, nodeName, sql, res);
        }

        private static RuntimeException reportError(String fileName, int lineNo, String message, Object... params) {
            String formatted = format(message, params);
            String fullErrorMessage = format("({}:{}): {}", fileName, lineNo, formatted);

            return new IllegalStateException(fullErrorMessage);
        }
    }
}
