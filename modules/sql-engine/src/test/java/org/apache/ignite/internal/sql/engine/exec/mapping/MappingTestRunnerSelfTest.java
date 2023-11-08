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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingTestRunner.TestCaseDef;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingTestRunner.TestSetup;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.opentest4j.AssertionFailedError;

/**
 * Tests for {@link MappingTestRunner}.
 */
public class MappingTestRunnerSelfTest extends BaseIgniteAbstractTest {

    private static final Path LOCATION = Paths.get("src/test/resources/mapping");

    @TempDir
    public Path tempDir;

    @Test
    public void testLoadTestCases() {
        List<TestCaseDef> testCaseDefs = MappingTestRunner.loadTestCases(LOCATION.resolve("_runner_self.test"));
        assertEquals(3, testCaseDefs.size());

        TestCaseDef testCase1 = testCaseDefs.get(0);

        String description = "# Start at node N1" + System.lineSeparator()
                + "# Query: S1 S1" + System.lineSeparator()
                + "# Fragments: F1";

        assertEquals(description, testCase1.description);
        assertEquals("N1", testCase1.nodeName);
        assertEquals("S1" + System.lineSeparator() +  " S1", testCase1.sql);
        assertEquals("Fragment#1" + System.lineSeparator() + "F1", testCase1.result);

        TestCaseDef testCase2 = testCaseDefs.get(1);
        assertNull(testCase2.description);
        assertEquals("N1", testCase2.nodeName);
        assertEquals("S2", testCase2.sql);
        assertEquals("Fragment#1" + System.lineSeparator() + "F2", testCase2.result);

        TestCaseDef testCase3 = testCaseDefs.get(2);
        assertNull(testCase3.description);
        assertEquals("N2", testCase3.nodeName);
        assertEquals("S3", testCase3.sql);
        assertEquals(
                "Fragment#1" + System.lineSeparator() + "F1" + System.lineSeparator().repeat(2)
                + "Fragment#2" + System.lineSeparator() + "F2", testCase3.result
        );
    }

    @Test
    @WithSystemProperty(key = "MAPPING_TESTS_OVERWRITE_RESULTS", value = "true")
    public void testOverwriteResults() throws IOException {
        Path file = tempDir.resolve("test0.test");

        List<TestCaseDef> testCaseDefs = MappingTestRunner.loadTestCases(LOCATION.resolve("_runner_self.test"));
        List<String> actualResults = testCaseDefs.stream()
                .map(r -> "RRR" + System.lineSeparator() + r.result)
                .collect(Collectors.toList());

        MappingTestRunner runner = newTestRunner();
        runner.initFlags();

        assertThrows(AssertionFailedError.class,
                () -> runner.reportResults(testCaseDefs, actualResults, file));

        String expectedStr = Files.readString(LOCATION.resolve("_runner_overwrite_results.test"), StandardCharsets.UTF_8);
        String actualStr = Files.readString(file, StandardCharsets.UTF_8);
        assertEquals(expectedStr, actualStr);
    }

    @Test
    @WithSystemProperty(key = "MAPPING_TESTS_STRIP_RESULTS", value = "true")
    public void testStripResults() throws IOException {
        Path file = tempDir.resolve("test0.test");

        List<TestCaseDef> testCaseDefs = MappingTestRunner.loadTestCases(LOCATION.resolve("_runner_self.test"));
        List<String> actualResults = testCaseDefs.stream().map(r -> r.result).collect(Collectors.toList());

        MappingTestRunner runner = newTestRunner();
        runner.initFlags();

        runner.reportResults(testCaseDefs, actualResults, file);

        String expectedStr = Files.readString(LOCATION.resolve("_runner_strip_results.test"), StandardCharsets.UTF_8);
        String actualStr = Files.readString(file, StandardCharsets.UTF_8);
        assertEquals(expectedStr, actualStr);
    }

    @Test
    @WithSystemProperty(key = "MAPPING_TESTS_OVERWRITE_RESULTS", value = "true")
    @WithSystemProperty(key = "MAPPING_TESTS_STRIP_RESULTS", value = "true")
    public void testIncompatibleFlags() {
        Path file = tempDir.resolve("test0.test");

        // overwriteResults and stripResults flags can not be used together.

        MappingTestRunner runner = newTestRunner().outputFile(file);

        IllegalStateException err = assertThrows(IllegalStateException.class,
                () -> runner.runTest(() -> {
                    ExecutionTargetProvider targetProvider = Mockito.mock(ExecutionTargetProvider.class);
                    IgniteSchema schema = new IgniteSchema("T", 1, List.of());
                    LogicalNode node = new LogicalNode("N1", "N1", new NetworkAddress("addr", 1000));
                    LogicalTopologySnapshot topologySnapshot = new LogicalTopologySnapshot(1, List.of(node));

                    return new TestSetup(targetProvider, schema, topologySnapshot);
                }, "_runner_self.test"));

        String error = "Both MAPPING_TESTS_OVERWRITE_RESULTS and MAPPING_TESTS_STRIP_RESULTS have been specified";
        assertThat(err.getMessage(), containsString(error));
    }

    private static MappingTestRunner newTestRunner() {
        return new MappingTestRunner(LOCATION, (schema, sqlStmt) -> {
            return Mockito.mock(IgniteRel.class);
        });
    }
}
