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

package org.apache.ignite.internal.cli.commands.sql;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.commands.TopLevelCliReplCommand;
import org.apache.ignite.internal.cli.core.repl.SessionDefaultValueProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/** Base class for testing CLI REPL sql command in the connected state. */
public class CliSqlConnectCommandTestBase extends CliSqlCommandTestBase {
    @Inject
    private SessionDefaultValueProvider defaultValueProvider;

    @BeforeEach
    @Override
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        commandLine().setDefaultValueProvider(defaultValueProvider);
    }

    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliReplCommand.class;
    }

}
