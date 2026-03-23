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

package org.apache.ignite.internal.cli.commands;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for non-initialized cluster fir Non-REPL mode.
 */
public class ItNotInitializedClusterTest extends CliCommandTestNotInitializedIntegrationBase  {

    protected String getExpectedErrorMessage() {
        return CLUSTER_NOT_INITIALIZED_ERROR_MESSAGE;
    }

    private static String testFile;

    @BeforeAll
    static void beforeAll() throws IOException {
        testFile = Files.createFile(WORK_DIR.resolve("test.txt")).toString();
    }

    private static Stream<Arguments> commands() {
        return Stream.of(
                arguments("node metric list", "Cannot list metrics"),
                arguments("node metric source enable metricName", "Cannot enable metrics"),
                arguments("node metric source disable metricName", "Cannot disable metrics"),
                arguments("node metric source list", "Cannot list metric sources"),
                arguments("node unit list", "Cannot list units"),
                arguments("cluster config show", "Cannot show cluster config"),
                arguments("cluster config update test", "Cannot update cluster config"),
                arguments("cluster topology logical", "Cannot show logical topology"),
                arguments("cluster unit deploy test.unit.id.1 --version 1.0.0 --path " + testFile, "Cannot deploy unit"),
                arguments("cluster unit undeploy test.unit.id.2 --version 1.0.0", "Cannot undeploy unit"),
                arguments("cluster unit list", "Cannot list units"),
                arguments("cluster metric source enable metricName", "Cannot enable metrics"),
                arguments("cluster metric source disable metricName", "Cannot disable metrics"),
                arguments("cluster metric source list", "Cannot list metric sources"),
                arguments("recovery partitions restart --zone zoneName", "Cannot restart partitions"),
                arguments("recovery partitions reset --zone zoneName", "Cannot reset partitions"),
                arguments("recovery partitions states --global", "Cannot list partition states")
        );
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("commands")
    void test(String argLine, String expectedErrorMessage) {
        execute(argLine.split(" "));

        assertAll(
                this::assertExitCodeIsError,
                () -> assertErrOutputIs(expectedErrorMessage + System.lineSeparator()
                        + getExpectedErrorMessage() + System.lineSeparator())
        );
    }
}
