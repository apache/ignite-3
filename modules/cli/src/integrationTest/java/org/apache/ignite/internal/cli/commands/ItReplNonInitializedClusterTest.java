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

import org.junit.jupiter.api.BeforeEach;

/**
 * Tests for non-initialized cluster fir REPL mode.
 */
public class ItReplNonInitializedClusterTest extends ItNonInitializedClusterTest {

    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliReplCommand.class;
    }

    @BeforeEach
    void connect() {
        execute("connect");

        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300" + System.lineSeparator()
                + "The cluster is not initialized. Run cluster init command to initialize it.")
        );
    }

    @Override
    protected String getExpectedErrorMessage() {
        return CLUSTER_NOT_INITIALIZED_REPL_ERROR_MESSAGE;
    }
}
