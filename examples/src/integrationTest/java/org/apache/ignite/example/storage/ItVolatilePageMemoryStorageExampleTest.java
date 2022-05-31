/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.example.storage;

import static org.apache.ignite.example.ExampleTestUtils.assertConsoleOutputContains;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.example.AbstractExamplesTest;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PageMemoryStorageEngineConfiguration;
import org.junit.jupiter.api.Test;

/**
 * For {@link VolatilePageMemoryStorageExample} testing.
 */
public class ItVolatilePageMemoryStorageExampleTest extends AbstractExamplesTest {
    @Test
    public void testExample() throws Exception {
        ignite
                .clusterConfiguration()
                .getConfiguration(PageMemoryStorageEngineConfiguration.KEY)
                .regions()
                .change(regionsChange -> regionsChange.create("in-memory", regionChange -> regionChange.changePersistent(false)))
                .get(1, TimeUnit.SECONDS);

        assertConsoleOutputContains(VolatilePageMemoryStorageExample::main, EMPTY_ARGS,
                "\nAll accounts:\n"
                        + "    John, Doe, Forest Hill\n"
                        + "    Jane, Roe, Forest Hill\n"
                        + "    Mary, Major, Denver\n"
                        + "    Richard, Miles, St. Petersburg\n"
        );
    }
}
