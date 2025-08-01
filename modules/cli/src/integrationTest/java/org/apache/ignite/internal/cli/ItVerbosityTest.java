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

package org.apache.ignite.internal.cli;

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.cli.commands.node.config.NodeConfigShowCommand;
import org.junit.jupiter.api.Test;

class ItVerbosityTest extends CliIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected Class<?> getCommandClass() {
        return NodeConfigShowCommand.class;
    }

    @Test
    void noVerbose() {
        execute("--url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    void verboseBasic() {
        execute("--url", NODE_URL, "-v");

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertErrOutputContains("--> GET"),
                () -> assertErrOutputDoesNotContain("Accept: application/problem+json"),
                () -> assertErrOutputDoesNotContain("{\"ignite\"")
        );
    }

    @Test
    void verboseHeaders() {
        execute("--url", NODE_URL, "-vv");

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertErrOutputContains("--> GET"),
                () -> assertErrOutputContains("Accept: application/problem+json"),
                () -> assertErrOutputDoesNotContain("{\"ignite\"")
        );
    }

    @Test
    void verboseBody() {
        execute("--url", NODE_URL, "-vvv");

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertErrOutputContains("--> GET"),
                () -> assertErrOutputContains("Accept: application/problem+json"),
                () -> assertErrOutputContains("{\"ignite\"")
        );
    }
}
