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

package org.apache.ignite.internal.cli.decorators;

import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.sql.SqlQueryResult;

/**
 * Composite decorator for {@link SqlQueryResult}.
 */
public class SqlQueryResultDecorator implements Decorator<SqlQueryResult, TerminalOutput> {
    private final boolean plain;
    private final boolean timed;
    private final TruncationConfig truncationConfig;

    public SqlQueryResultDecorator(boolean plain) {
        this(plain, false, TruncationConfig.disabled());
    }

    public SqlQueryResultDecorator(boolean plain, boolean timed) {
        this(plain, timed, TruncationConfig.disabled());
    }

    /**
     * Creates a new SqlQueryResultDecorator with truncation support.
     *
     * @param plain whether to use plain formatting
     * @param timed whether to include execution time
     * @param truncationConfig truncation configuration
     */
    public SqlQueryResultDecorator(boolean plain, boolean timed, TruncationConfig truncationConfig) {
        this.plain = plain;
        this.timed = timed;
        this.truncationConfig = truncationConfig;
    }

    @Override
    public TerminalOutput decorate(SqlQueryResult data) {
        return data.getResult(plain, timed, truncationConfig);
    }
}
