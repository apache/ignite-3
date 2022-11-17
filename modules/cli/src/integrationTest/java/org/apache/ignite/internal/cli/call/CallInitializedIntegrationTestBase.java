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

package org.apache.ignite.internal.cli.call;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;

import org.apache.ignite.internal.cli.IntegrationTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;

/**
 * Base class for call integration tests that needs initialized ignite cluster. Contains common methods and useful assertions.
 */
public class CallInitializedIntegrationTestBase extends IntegrationTestBase {
    @BeforeAll
    void beforeAll(TestInfo testInfo) {
        startNodes(testInfo);
        String metaStorageNodeName = testNodeName(testInfo, 0);
        initializeCluster(metaStorageNodeName);
    }

    @AfterAll
    void afterAll(TestInfo testInfo) throws Exception {
        stopNodes(testInfo);
    }
}
