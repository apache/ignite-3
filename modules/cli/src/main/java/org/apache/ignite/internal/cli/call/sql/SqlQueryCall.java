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

package org.apache.ignite.internal.cli.call.sql;

import java.sql.SQLException;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.call.StringCallInput;
import org.apache.ignite.internal.cli.sql.SqlManager;
import org.apache.ignite.internal.cli.sql.SqlQueryResult;

/**
 * Call implementation for SQL command execution.
 */
public class SqlQueryCall implements Call<StringCallInput, SqlQueryResult> {

    private final SqlManager sqlManager;

    /**
     * Constructor.
     *
     * @param sqlManager SQL manager.
     */
    public SqlQueryCall(SqlManager sqlManager) {
        this.sqlManager = sqlManager;
    }

    /** {@inheritDoc} */
    @Override
    public CallOutput<SqlQueryResult> execute(StringCallInput input) {
        try {
            SqlQueryResult result = sqlManager.execute(trimQuotes(input.getString()));
            return DefaultCallOutput.success(result);
        } catch (SQLException e) {
            return DefaultCallOutput.failure(e);
        }
    }

    private static String trimQuotes(String input) {
        if (input.startsWith("\"") && input.endsWith("\"") && input.length() > 2) {
            return input.substring(1, input.length() - 1);
        }
        return input;
    }
}
