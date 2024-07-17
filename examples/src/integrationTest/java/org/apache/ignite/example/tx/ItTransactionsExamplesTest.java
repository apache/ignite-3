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

package org.apache.ignite.example.tx;

import static org.apache.ignite.example.ExampleTestUtils.assertConsoleOutputContains;

import org.apache.ignite.example.AbstractExamplesTest;
import org.junit.jupiter.api.Test;

/**
 * Tests for transactional examples.
 */
public class ItTransactionsExamplesTest extends AbstractExamplesTest {
    /**
     * Runs TransactionsExample and checks its output.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTransactionsExample() throws Exception {
        assertConsoleOutputContains(TransactionsExample::main, EMPTY_ARGS,
                "Initial balance: 1000.0",
                "Balance after the sync transaction: 1200.0",
                "Balance after the async transaction: 1500.0");
    }
}
