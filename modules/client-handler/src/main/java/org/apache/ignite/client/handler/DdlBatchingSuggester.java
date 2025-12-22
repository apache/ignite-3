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

package org.apache.ignite.client.handler;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.jetbrains.annotations.TestOnly;

/**
 * Tracks the number of sequential DDL queries executed and prints
 * the recommendation to use batch processing of DDL queries.
 */
public class DdlBatchingSuggester implements Consumer<SqlQueryType> {
    /** The number of DDL commands, after which it is necessary to print the recommendation for the user. */
    static final int THRESHOLD = 10;

    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DdlBatchingSuggester.class);

    /** Message printer. */
    private final Consumer<String> printer;

    /** Number of DDL queries processed in a row. */
    private final AtomicInteger counter = new AtomicInteger();

    DdlBatchingSuggester() {
        this.printer = LOG::warn;
    }

    @TestOnly
    DdlBatchingSuggester(Consumer<String> printer) {
        this.printer = printer;
    }

    @Override
    public void accept(SqlQueryType type) {
        if (type != SqlQueryType.DDL) {
            counter.set(0);

            return;
        }

        if (counter.incrementAndGet() == THRESHOLD) {
            printer.accept("Multiple DDL statements were executed individually. For improved performance, "
                    + "consider grouping DDL statements into a single SQL script. To disable this suggestion, "
                    + "set the cluster property 'ignite.suggestions.sequentialDdlExecution.enabled' to 'false'.");
        }
    }

    @TestOnly
    public int trackedQueriesCount() {
        return counter.get();
    }
}
