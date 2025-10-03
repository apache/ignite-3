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

package org.apache.ignite.migrationtools.tests.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.migrationtools.tests.bases.MigrationTestBase;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.DiscoveryUtils;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.ExampleBasedCacheTest;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.NameUtils;
import org.apache.ignite.migrationtools.tests.e2e.framework.core.SqlTest;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.Table;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;

/**
 * End-to-End test that uses the CLI to execute a cache migration and executes the example tests afterward.
 * The tests are executed against a AI3 cluster with the native client and the JDBC client.
 */
public class DataMigrationWithIgnite3ClientTest extends MigrationTestBase {
    // TODO: Check if it is possible to rename the tests
    // TODO: Some of these properties are duplicated with other tests.

    private static final List<ExampleBasedCacheTest> TEST_CLASS_INSTANCES = DiscoveryUtils.discoverClasses();

    private static int N_TEST_EXAMPLES;

    private static int numberOfTestsLimiter;

    @BeforeAll
    static void setNumTestSamples() {
        var numTestSamples = System.getenv("N_TEST_SAMPLES");
        if (numTestSamples != null) {
            N_TEST_EXAMPLES = Integer.parseUnsignedInt(numTestSamples);
        } else {
            N_TEST_EXAMPLES = 2_500;
        }
    }

    @BeforeAll
    static void setNumberOfTestsLimiter() {
        numberOfTestsLimiter = Integer.parseUnsignedInt(System.getProperty("e2e.testLimiter", String.valueOf(Integer.MAX_VALUE)));
    }

    @BeforeAll
    static void enableInfoLogsUnconditionally() {
        // TODO: Check if this is a good idea. It was quick
        Configurator.setLevel(LogManager.getLogger(MigrationTestBase.class), Level.INFO);
    }

    @BeforeAll
    static void loadJdbcDriver() {
        ServiceLoader.load(org.apache.ignite.jdbc.IgniteJdbcDriver.class);
    }

    @TestFactory
    Stream<DynamicNode> compoundTest() throws Exception {
        var ai3ClientBuilder = IgniteClient.builder().addresses(MigrationTestBase.AI3_CLUSTER.getAddress());
        var jdbcAddr = "jdbc:ignite:thin://" + MigrationTestBase.AI3_CLUSTER.getAddress();

        return IntStream.range(0, TEST_CLASS_INSTANCES.size())
                .mapToObj(i -> {
                    ExampleBasedCacheTest<?, ?> test = TEST_CLASS_INSTANCES.get(i);

                    // These assumption are pre-computed because they are the same for all tests, we might as well compute them only once.
                    List<Executable> assumptions = new ArrayList<>();
                    {
                        if (i > numberOfTestsLimiter) {
                            assumptions.add(() -> Assumptions.abort("Number of tests limited to: " + numberOfTestsLimiter));
                        }
                    }

                    Function<Executable, Executable> withAssumptions = exec -> () -> {
                        for (Executable ex : assumptions) {
                            ex.execute();
                        }

                        exec.execute();
                    };

                    Executable migrationTest = withAssumptions.apply(
                            () -> MigrationTestBase.migrationIsSuccessfull(test.getTableName(), "IGNORE_COLUMN")
                    );
                    Executable apiTest = withAssumptions.apply(() -> runIgnite3ApiTest(ai3ClientBuilder, test));
                    Stream<DynamicTest> jdbcTests = test.jdbcTests().entrySet().stream()
                            .map(sqlTestEntry -> {
                                Map.Entry<String, SqlTest> entry = (Map.Entry<String, SqlTest>) sqlTestEntry;
                                String name = entry.getKey();
                                SqlTest sqlTest = entry.getValue();
                                return dynamicTest(name, withAssumptions.apply(() -> runJdbcTest(jdbcAddr, sqlTest)));
                            });

                    return dynamicContainer(
                            String.format("T:%d :: %s :: %s", i + 1, test.getClass().getName(), test.getTableName()),
                            Stream.of(
                                    dynamicTest("migrationIsSuccessfull", migrationTest),
                                    dynamicTest("Ignite 3 java API", apiTest),
                                    dynamicContainer("JDBC Tests", jdbcTests)
                            ));
                });
    }

    private static void runIgnite3ApiTest(
            IgniteClient.Builder clientBuilder,
            ExampleBasedCacheTest<?, ?> exampleBasedTest
    ) throws Exception {
        try (IgniteClient client = clientBuilder.build()) {
            var qn = QualifiedName.parse(NameUtils.ignite3TableName(exampleBasedTest));
            Table table = client.tables().table(qn);
            assertThat(table)
                    .withFailMessage("Table with name must exist: " + qn)
                    .isNotNull();

            exampleBasedTest.testIgnite3(table, N_TEST_EXAMPLES);
        }
    }

    private static void runJdbcTest(String jdbcConnectionStr, SqlTest sqlTest) throws SQLException {
        try (var conn = DriverManager.getConnection(jdbcConnectionStr)) {
            sqlTest.test(conn, N_TEST_EXAMPLES);
        }
    }
}
