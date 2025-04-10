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

package org.apache.ignite.migrationtools.handlers;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;

/** Automatically skips tests if based on exceptions thrown for unimplemented features. */
public class SkipUnsupportedOperationsHandlers implements TestExecutionExceptionHandler {
    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        if (throwable instanceof UnsupportedOperationException
                && throwable.getMessage().contains("Only SQL Queries are currently supported")) {
            // TODO: GG-39659
            Assumptions.abort("This test requires support for non-SQL queries. (TODO: GG-39659)");
            return;
        }

        if (throwable instanceof UnsupportedOperationException
                && throwable.getMessage().contains("Query with _KEY and _VAL columns is not supported")) {
            // TODO: GG-40624
            Assumptions.abort("This test requires support for ai2 internal columns (TODO: GG-40624)");
            return;
        }

        if (throwable instanceof RuntimeException
                && throwable.getMessage().contains("org.apache.calcite.sql.parser.SqlParseException: Encountered \"REGEXP\"")) {
            // TODO: GG-40623
            Assumptions.abort("This test requires support for H2 REGEXP operator (TODO: GG-40623)");
            return;
        }

        throw throwable;
    }
}
