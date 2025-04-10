/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
