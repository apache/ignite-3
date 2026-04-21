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

package org.apache.ignite.example.table;

import static org.apache.ignite.example.ExampleTestUtils.assertConsoleOutputContains;

import org.apache.ignite.example.AbstractExamplesTest;
import org.junit.jupiter.api.Test;

/**
 * These tests check that all table examples pass correctly.
 */
public class ItTableExamplesTest extends AbstractExamplesTest {
    /**
     * Runs RecordViewExample.
     *
     * @throws Exception If failed and checks its output.
     */
    @Test
    public void testRecordViewExample() throws Exception {
        assertConsoleOutputContains(RecordViewExample::main, EMPTY_ARGS,
                "\nRetrieved record:\n"
                        + "    Account Number: 123456\n"
                        + "    Owner: Jane Doe\n"
                        + "    Balance: $100.0\n");
    }

    /**
     * Runs RecordViewPojoExample.
     *
     * @throws Exception If failed and checks its output.
     */
    @Test
    public void testRecordViewPojoExample() throws Exception {
        assertConsoleOutputContains(RecordViewPojoExample::main, EMPTY_ARGS,
                "\nRetrieved record:\n"
                        + "    Account Number: 123456\n"
                        + "    Owner: Jane Doe\n"
                        + "    Balance: $100.0\n");
    }

    /**
     * Runs KeyValueViewExample and checks its output.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testKeyValueViewExample() throws Exception {
        assertConsoleOutputContains(KeyValueViewExample::main, EMPTY_ARGS,
                "\nRetrieved value:\n"
                        + "    Account Number: 123456\n"
                        + "    Owner: Jane Doe\n"
                        + "    Balance: $100.0\n");
    }

    /**
     * Runs KeyValueViewPojoExample and checks its output.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testKeyValueViewPojoExample() throws Exception {
        assertConsoleOutputContains(KeyValueViewPojoExample::main, EMPTY_ARGS,
                "\nRetrieved value:\n"
                        + "    Account Number: 123456\n"
                        + "    Owner: Jane Doe\n"
                        + "    Balance: $100.0\n");
    }
}
