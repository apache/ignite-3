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

package org.apache.ignite.internal.client;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.CompatibilityTestBase;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;

/**
 * Tests that current Java client can work with all older server versions.
 */
public class CurrentClientWithOldServerCompatibilityTest extends CompatibilityTestBase implements ClientCompatibilityTests {
    private final AtomicInteger idGen = new AtomicInteger();

    private @Nullable IgniteClient client;

    @Override
    protected void setupBaseVersion(Ignite baseIgnite) {
        createDefaultTables();
    }

    @Override
    protected int nodesCount() {
        return 1;
    }

    @Override
    public IgniteClient client() {
        if (client == null) {
            client = IgniteClient.builder()
                    .addresses("localhost:10800")
                    .build();
        }

        return client;
    }

    @AfterEach
    public void afterEach() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public AtomicInteger idGen() {
        return idGen;
    }
}
