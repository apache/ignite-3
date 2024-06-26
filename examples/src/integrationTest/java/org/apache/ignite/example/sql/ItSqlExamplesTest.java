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

package org.apache.ignite.example.sql;

import static org.apache.ignite.example.ExampleTestUtils.assertConsoleOutputContains;

import org.apache.ignite.example.AbstractExamplesTest;
import org.apache.ignite.example.sql.jdbc.SqlJdbcExample;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * These tests check that all SQL JDBC examples pass correctly.
 */
public class ItSqlExamplesTest extends AbstractExamplesTest {
    /**
     * Runs SqlJdbcExample and checks its output.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSqlJdbcExample() throws Exception {
        assertConsoleOutputContains(SqlJdbcExample::main, EMPTY_ARGS,
                "\nAll accounts:\n"
                        + "    John, Doe, Forest Hill\n"
                        + "    Jane, Roe, Forest Hill\n"
                        + "    Mary, Major, Denver\n"
                        + "    Richard, Miles, St. Petersburg\n",

                "\nAccounts with balance lower than 1,500:\n"
                        + "    John, Doe, 1000.0\n"
                        + "    Richard, Miles, 1450.0\n",

                "\nAll accounts:\n"
                        + "    Jane, Roe, Forest Hill\n"
                        + "    Mary, Major, Denver\n"
                        + "    Richard, Miles, St. Petersburg\n"
        );
    }

    /**
     * Runs SqlApiExample and checks its output.
     *
     * @throws Exception If failed.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22262")
    @Test
    public void testSqlApiExample() throws Exception {
        assertConsoleOutputContains(SqlApiExample::main, EMPTY_ARGS,
                "\nAdded cities: 3",
                "\nAdded accounts: 4",

                "\nAll accounts:\n"
                        + "    John, Doe, Forest Hill\n"
                        + "    Jane, Roe, Forest Hill\n"
                        + "    Mary, Major, Denver\n"
                        + "    Richard, Miles, St. Petersburg\n",

                "\nAccounts with balance lower than 1,500:\n"
                        + "    John, Doe, 1000.0\n"
                        + "    Richard, Miles, 1450.0\n",

                "\nAll accounts:\n"
                        + "    Jane, Roe, Forest Hill\n"
                        + "    Mary, Major, Denver\n"
                        + "    Richard, Miles, St. Petersburg\n"
        );
    }
}
